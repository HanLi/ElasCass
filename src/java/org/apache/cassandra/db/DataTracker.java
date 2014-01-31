/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DataTracker
{
    private static final Logger logger = LoggerFactory.getLogger(DataTracker.class);

    public Collection<INotificationConsumer> subscribers = new CopyOnWriteArrayList<INotificationConsumer>();

    public final ColumnFamilyStore cfstore;

    private final AtomicReference<View> view;

    // On disk live and total size
    private final AtomicLong liveSize = new AtomicLong();
    private final AtomicLong totalSize = new AtomicLong();
    
    private final Map<Token, AtomicLong> hitCount = Maps.newHashMap();

    public DataTracker(ColumnFamilyStore cfstore)
    {
        this.cfstore = cfstore;
        this.view = new AtomicReference<View>();
        this.init();
    }

    public Memtable getMemtable()
    {
        return view.get().memtable;
    }

    public Set<Memtable> getMemtablesPendingFlush()
    {
        return view.get().memtablesPendingFlush;
    }

    /** the given range should be presented in the data view, rather than any random range */
    public Collection<SSTableReader> getSSTables(Range range)
    {
        return view.get().sstables.get(range);
    }
    
    public Collection<SSTableReader> getAllSSTables()
    {
        return view.get().sstables.values();
    }
    
    public Map<Range, Collection<SSTableReader>> getSSTablesInBuckets()
    {
        return view.get().sstables.asMap();
    }

    public View getView()
    {
        return view.get();
    }
    
    public List<SSTableReader> getIntervalTree(Token canonicalToken)
    {
    	AtomicLong count = getHitCountFor(canonicalToken);
    	count.incrementAndGet();
    	return view.get().intervalTree.get(canonicalToken);
    }
    
    public List<SSTableReader> getIntervalTree(DecoratedKey key)
    {
    	View currentView = view.get();
    	Token token = currentView.getInternalToken(key.token);
    	if(token != null)
    	{
    		AtomicLong count = getHitCountFor(token);
        	count.incrementAndGet();
        	return currentView.intervalTree.get(token);
    	}
    	
    	return Collections.<SSTableReader>emptyList();
    }
    
    public AtomicLong getHitCountFor(Token canonicalToken)
    {
    	AtomicLong count;
    	if(hitCount.containsKey(canonicalToken))
    	{
    		count = hitCount.get(canonicalToken);
    	}
    	else
    	{
    		count = new AtomicLong(0);
    		hitCount.put(canonicalToken, count);
    	}
    	return count;
    }
    
    
    public Map<Token, Long> getAndClearHitCounts()
    {
    	Map<Token, Long> map = Maps.newHashMap();
    	for(Map.Entry<Token, AtomicLong> count : hitCount.entrySet())
    	{
    		long value = count.getValue().getAndSet(0);
    		if(value > 0)
    			map.put(count.getKey(), value);
    	}
    	return map;
    }
    
    
    /**
     * Switch the current memtable.
     * This atomically adds the current memtable to the memtables pending
     * flush and replace it with a fresh memtable.
     *
     * @return the previous current memtable (the one added to the pending
     * flush)
     */
    public Memtable switchMemtable()
    {
        // atomically change the current memtable
        Memtable newMemtable = new Memtable(cfstore);
        Memtable toFlushMemtable;
        View currentView, newView;
        do
        {
            currentView = view.get();
            toFlushMemtable = currentView.memtable;
            newView = currentView.switchMemtable(newMemtable);
        }
        while (!view.compareAndSet(currentView, newView));

        return toFlushMemtable;
    }

    /**
     * Renew the current memtable without putting the old one for a flush.
     * Used when we flush but a memtable is clean (in which case we must
     * change it because it was frozen).
     */
    public void renewMemtable()
    {
        Memtable newMemtable = new Memtable(cfstore);
        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.renewMemtable(newMemtable);
        }
        while (!view.compareAndSet(currentView, newView));
    }

    public void replaceFlushed(Memtable memtable, Map<Range, SSTableReader> ssTables)
    {
        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replaceFlushed(memtable, ssTables);
        }
        while (!view.compareAndSet(currentView, newView));

        addNewSSTablesSize(ssTables.values());
        cfstore.updateCacheSizes();
        
        for(SSTableReader ssTable : ssTables.values())
        {
        	notifyAdded(ssTable);
            incrementallyBackup(ssTable);
        }
    }

    public void incrementallyBackup(final SSTableReader sstable)
    {
        if (!DatabaseDescriptor.incrementalBackupsEnabled())
            return;

        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                File keyspaceDir = new File(sstable.getFilename()).getParentFile();
                File backupsDir = new File(keyspaceDir, "backups");
                if (!backupsDir.exists() && !backupsDir.mkdirs())
                    throw new IOException("Unable to create " + backupsDir);
                sstable.createLinks(backupsDir.getCanonicalPath());
            }
        };
        StorageService.tasks.execute(runnable);
    }
    
    public boolean isSplittable(Range range)
    {
    	View currentView = view.get();
    	return !currentView.compacting.containsKey(range);
    }

    public Set<SSTableReader> getMarkedForSplitting(Range range)
    {
    	Set<SSTableReader> subset = null;
    	View currentView, newView;
        do
        {
            currentView = view.get();
            
            List<SSTableReader> tomark = currentView.sstables.get(range);
            subset = new HashSet<SSTableReader>(tomark);
            subset.removeAll(currentView.compacting.get(range));
            
            if (subset.isEmpty())
                break;

            newView = currentView.markCompacting(subset);
        }
        while (!view.compareAndSet(currentView, newView));
        return subset;
    }
    
    /**
     * @return A subset of the given active sstables that have been marked compacting,
     * or null if the thresholds cannot be met: files that are marked compacting must
     * later be unmarked using unmarkCompacting.
     *
     * Note that we could acquire references on the marked sstables and release them in
     * unmarkCompacting, but since we will never call markCompacted on a sstable marked
     * as compacting (unless there is a serious bug), we can skip this.
     */
    public Set<SSTableReader> markCompacting(Collection<SSTableReader> tomark, int min, int max)
    {
        if (max < min || max < 1)
            return null;
        if (tomark == null || tomark.isEmpty())
            return null;

        View currentView, newView;
        Set<SSTableReader> subset = null;
        // order preserving set copy of the input
        Set<SSTableReader> remaining = new LinkedHashSet<SSTableReader>(tomark);
        do
        {
            currentView = view.get();

            // find the subset that is active and not already compacting
            remaining.removeAll(currentView.compacting.values());
            remaining.retainAll(currentView.sstables.values());
            if (remaining.size() < min)
                // cannot meet the min threshold
                return null;

            // cap the newly compacting items into a subset set
            subset = new HashSet<SSTableReader>();
            Iterator<SSTableReader> iter = remaining.iterator();
            for (int added = 0; added < max && iter.hasNext(); added++)
                subset.add(iter.next());

            newView = currentView.markCompacting(subset);
        }
        while (!view.compareAndSet(currentView, newView));
        return subset;
    }

    /**
     * Removes files from compacting status: this is different from 'markCompacted'
     * because it should be run regardless of whether a compaction succeeded.
     */
    public void unmarkCompacting(Collection<SSTableReader> unmark)
    {
        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.unmarkCompacting(unmark);
        }
        while (!view.compareAndSet(currentView, newView));
    }

    public void markCompacted(Collection<SSTableReader> sstables)
    {
        replace(sstables, Collections.<SSTableReader>emptyList());
        notifySSTablesChanged(sstables, Collections.<SSTableReader>emptyList());
    }
    
    public boolean isCompactable(Range range, Collection<SSTableReader> sstables)
    {
    	Set<SSTableReader> remaining = new HashSet<SSTableReader>(sstables);
    	View currentView = view.get();
    	remaining.removeAll(currentView.compacting.get(range));
    	return !remaining.isEmpty();
    }

    public void addInitialSSTables(Collection<SSTableReader> sstables)
    {
        replace(Collections.<SSTableReader>emptyList(), sstables);
        // no notifications or backup necessary
    }

    public void addSSTables(Collection<SSTableReader> sstables)
    {
        replace(Collections.<SSTableReader>emptyList(), sstables);
        for (SSTableReader sstable : sstables)
        {
            incrementallyBackup(sstable);
            notifyAdded(sstable);
        }
    }

    public void replaceCompactedSSTables(Collection<SSTableReader> sstables, Iterable<SSTableReader> replacements)
	{
	    replace(sstables, replacements);
	    notifySSTablesChanged(sstables, replacements);
	}

	public void removeSSTables(Range range)
    {
        Collection<SSTableReader> sstables = getSSTables(range);
        if (!sstables.isEmpty())
        {
        	replace(sstables, Collections.<SSTableReader>emptyList());
            notifySSTablesChanged(sstables, Collections.<SSTableReader>emptyList());
        }
    }
    
    public void removeAllSSTables()
    {
        Collection<SSTableReader> sstables = getAllSSTables();
        if (sstables.isEmpty())
        {
            // notifySSTablesChanged -> LeveledManifest.promote doesn't like a no-op "promotion"
            return;
        }

        replace(sstables, Collections.<SSTableReader>emptyList());
        notifySSTablesChanged(sstables, Collections.<SSTableReader>emptyList());
    }

    /** (Re)initializes the tracker, purging all references. */
    void init()
    {
        view.set(new View(new Memtable(cfstore)));
    }

    private void replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
    {
        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replace(oldSSTables, replacements);
        }
        while (!view.compareAndSet(currentView, newView));

        addNewSSTablesSize(replacements);
        removeOldSSTablesSize(oldSSTables);

        cfstore.updateCacheSizes();
    }

    private void addNewSSTablesSize(Iterable<SSTableReader> newSSTables)
    {
        for (SSTableReader sstable : newSSTables)
        {
            assert sstable.getKeySamples() != null;
            if (logger.isDebugEnabled())
                logger.debug(String.format("adding %s to list of files tracked for %s.%s",
                            sstable.descriptor, cfstore.table.name, cfstore.getColumnFamilyName()));
            long size = sstable.bytesOnDisk();
            liveSize.addAndGet(size);
            totalSize.addAndGet(size);
            sstable.setTrackedBy(this);
        }
    }

    private void removeOldSSTablesSize(Iterable<SSTableReader> oldSSTables)
    {
        for (SSTableReader sstable : oldSSTables)
        {
            if (logger.isDebugEnabled())
                logger.debug(String.format("removing %s from list of files tracked for %s.%s",
                            sstable.descriptor, cfstore.table.name, cfstore.getColumnFamilyName()));
            liveSize.addAndGet(-sstable.bytesOnDisk());
            sstable.markCompacted();
            sstable.releaseReference();
        }
    }

    public AutoSavingCache<Pair<Descriptor,DecoratedKey>,Long> getKeyCache()
    {
        return cfstore.getKeyCache();
    }

    public long getLiveSize()
    {
        return liveSize.get();
    }

    public long getTotalSize()
    {
        return totalSize.get();
    }

    public void spaceReclaimed(long size)
    {
        totalSize.addAndGet(-size);
    }

    public long estimatedKeys()
    {
        long n = 0;
        for (SSTableReader sstable : getAllSSTables())
        {
            n += sstable.estimatedKeys();
        }
        return n;
    }

    public long[] getEstimatedRowSizeHistogram()
    {
        long[] histogram = new long[90];

        for (SSTableReader sstable : getAllSSTables())
        {
            long[] rowSize = sstable.getEstimatedRowSize().getBuckets(false);

            for (int i = 0; i < histogram.length; i++)
                histogram[i] += rowSize[i];
        }

        return histogram;
    }

    public long[] getEstimatedColumnCountHistogram()
    {
        long[] histogram = new long[90];

        for (SSTableReader sstable : getAllSSTables())
        {
            long[] columnSize = sstable.getEstimatedColumnCount().getBuckets(false);

            for (int i = 0; i < histogram.length; i++)
                histogram[i] += columnSize[i];
        }

        return histogram;
    }

    public double getCompressionRatio()
    {
        double sum = 0;
        int total = 0;
        for (SSTableReader sstable : getAllSSTables())
        {
            if (sstable.getCompressionRatio() != Double.MIN_VALUE)
            {
                sum += sstable.getCompressionRatio();
                total++;
            }
        }
        return total != 0 ? (double)sum/total: 0;
    }

    public long getMinRowSize()
    {
        long min = 0;
        for (SSTableReader sstable : getAllSSTables())
        {
            if (min == 0 || sstable.getEstimatedRowSize().min() < min)
                min = sstable.getEstimatedRowSize().min();
        }
        return min;
    }

    public long getMaxRowSize()
    {
        long max = 0;
        for (SSTableReader sstable : getAllSSTables())
        {
            if (sstable.getEstimatedRowSize().max() > max)
                max = sstable.getEstimatedRowSize().max();
        }
        return max;
    }

    public long getMeanRowSize()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getAllSSTables())
        {
            sum += sstable.getEstimatedRowSize().mean();
            count++;
        }
        return count > 0 ? sum / count : 0;
    }

    public int getMeanColumns()
    {
        long sum = 0;
        int count = 0;
        for (SSTableReader sstable : getAllSSTables())
        {
            sum += sstable.getEstimatedColumnCount().mean();
            count++;
        }
        return count > 0 ? (int) (sum / count) : 0;
    }

    public long getBloomFilterFalsePositives()
    {
        long count = 0L;
        for (SSTableReader sstable: getAllSSTables())
        {
            count += sstable.getBloomFilterFalsePositiveCount();
        }
        return count;
    }

    public long getRecentBloomFilterFalsePositives()
    {
        long count = 0L;
        for (SSTableReader sstable: getAllSSTables())
        {
            count += sstable.getRecentBloomFilterFalsePositiveCount();
        }
        return count;
    }

    public double getBloomFilterFalseRatio()
    {
        long falseCount = 0L;
        long trueCount = 0L;
        for (SSTableReader sstable: getAllSSTables())
        {
            falseCount += sstable.getBloomFilterFalsePositiveCount();
            trueCount += sstable.getBloomFilterTruePositiveCount();
        }
        if (falseCount == 0L && trueCount == 0L)
            return 0d;
        return (double) falseCount / (trueCount + falseCount);
    }

    public double getRecentBloomFilterFalseRatio()
    {
        long falseCount = 0L;
        long trueCount = 0L;
        for (SSTableReader sstable: getAllSSTables())
        {
            falseCount += sstable.getRecentBloomFilterFalsePositiveCount();
            trueCount += sstable.getRecentBloomFilterTruePositiveCount();
        }
        if (falseCount == 0L && trueCount == 0L)
            return 0d;
        return (double) falseCount / (trueCount + falseCount);
    }

    public void notifySSTablesChanged(Iterable<SSTableReader> removed, Iterable<SSTableReader> added)
    {
        for (INotificationConsumer subscriber : subscribers)
        {
            INotification notification = new SSTableListChangedNotification(added, removed);
            subscriber.handleNotification(notification, this);
        }
    }

    public void notifyAdded(SSTableReader added)
    {
        for (INotificationConsumer subscriber : subscribers)
        {
            INotification notification = new SSTableAddedNotification(added);
            subscriber.handleNotification(notification, this);
        }
    }

    public void subscribe(INotificationConsumer consumer)
    {
        subscribers.add(consumer);
    }

    public void unsubscribe(INotificationConsumer consumer)
    {
        boolean found = subscribers.remove(consumer);
        assert found : consumer + " not subscribed";
    }

    /**
     * An immutable structure holding the current memtable, the memtables pending
     * flush, the sstables for a column family, and the sstables that are active
     * in compaction (a subset of the sstables).
     */
    static class View
    {
        public final Memtable memtable;
        public final Set<Memtable> memtablesPendingFlush;
        public final ArrayListMultimap<Range, SSTableReader> compacting;
        // We can't use a SortedSet here because "the ordering maintained by a sorted set (whether or not an
        // explicit comparator is provided) must be <i>consistent with equals</i>."  In particular,
        // ImmutableSortedSet will ignore any objects that compare equally with an existing Set member.
        // Obviously, dropping sstables whose max column timestamp happens to be equal to another's
        // is not acceptable for us.  So, we use a List instead.
        public final ArrayListMultimap<Range, SSTableReader> sstables;
        public final ArrayListMultimap<Token, SSTableReader> intervalTree;
        private final List<Token> sortedTokens;

        public static final Comparator<Token> tokenComparator = 
        	new Comparator<Token>()
        	{
				@Override
				public int compare(Token o1, Token o2) 
				{
					return o1.compareTo(o2);
				}};
        
        View(Memtable memtable)
        {
        	this(memtable, new HashSet<Memtable>(0),
        			ArrayListMultimap.<Range, SSTableReader>create(),
        			ArrayListMultimap.<Range, SSTableReader>create(),
        			ArrayListMultimap.<Token, SSTableReader>create(),
        			new ArrayList<Token>(0));
        }
        
        View(Memtable memtable, Set<Memtable> pendingFlush, 
        		ArrayListMultimap<Range, SSTableReader> sstables, 
        		ArrayListMultimap<Range, SSTableReader> compacting,
        		ArrayListMultimap<Token, SSTableReader> intervalTree,
        		List<Token> sortedTokens)
        {
            this.memtable = memtable;
            this.memtablesPendingFlush = pendingFlush;
            this.sstables = sstables;
            this.compacting = compacting;
            this.intervalTree = intervalTree;
            this.sortedTokens = sortedTokens;
        }
        
        private ArrayListMultimap<Token,SSTableReader> buildIntervalTree(ArrayListMultimap<Range, SSTableReader> listMap)
        {
        	ArrayListMultimap<Token, SSTableReader> newTree = ArrayListMultimap.<Token, SSTableReader>create();
        	for(Range range : listMap.keySet())
        	{
        		List<SSTableReader> ssTables = listMap.get(range);
        		newTree.putAll(range.right, ssTables);
        	}
        	return newTree;
        }
        
        private ArrayList<Token> sortTokens(Collection<Token> tokens)
        {
        	ArrayList<Token> tokenList = new ArrayList<Token>(tokens);
    		Collections.sort(tokenList, tokenComparator);
    		return tokenList;
        }
        
        public Token getInternalToken(Token token)
        {
        	if(sortedTokens.isEmpty())
        		return null;
        	
        	int index = Collections.binarySearch(sortedTokens, token, tokenComparator);
        	if (index < 0)
    		{
        		index = (index + 1) * (-1);
    			if (index >= sortedTokens.size())
    				index = 0;
    		}
        	
        	return sortedTokens.get(index);
        }

        public View switchMemtable(Memtable newMemtable)
        {
            Set<Memtable> newPending = ImmutableSet.<Memtable>builder().addAll(memtablesPendingFlush).add(memtable).build();
            return new View(newMemtable, newPending, sstables, compacting, intervalTree, sortedTokens);
        }

        public View renewMemtable(Memtable newMemtable)
        {
            return new View(newMemtable, memtablesPendingFlush, sstables, compacting, intervalTree, sortedTokens);
        }

        public View replaceFlushed(Memtable flushedMemtable, Map<Range, SSTableReader> ssTables)
        {
            Set<Memtable> newPending = ImmutableSet.copyOf(Sets.difference(memtablesPendingFlush, Collections.singleton(flushedMemtable)));
            ArrayListMultimap<Range, SSTableReader> newSSTables = newSSTables(ssTables);
            ArrayListMultimap<Token,SSTableReader> newTree = buildIntervalTree(newSSTables);
            ArrayList<Token> newTokens = sortTokens(newTree.keySet());
            return new View(memtable, newPending, newSSTables, compacting, newTree, newTokens);
        }

        public View replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
            ArrayListMultimap<Range,SSTableReader> newSSTables = newSSTables(oldSSTables, replacements);
            ArrayListMultimap<Token,SSTableReader> newTree = buildIntervalTree(newSSTables);
            ArrayList<Token> newTokens = sortTokens(newTree.keySet());
            return new View(memtable, memtablesPendingFlush, newSSTables, compacting, newTree, newTokens);
        }

        public View markCompacting(Iterable<SSTableReader> tomark)
        {
        	ArrayListMultimap<Range, SSTableReader> map = ArrayListMultimap.<Range, SSTableReader>create();
        	for(SSTableReader sstable : tomark)
        	{
        		map.put(sstable.getRange(), sstable);
        	}
            return markCompacting(map);
        }
        
        private View markCompacting(ArrayListMultimap<Range, SSTableReader> tomark)
        {
        	ArrayListMultimap<Range, SSTableReader> compactingNew = ArrayListMultimap.<Range, SSTableReader>create(compacting);
        	compactingNew.putAll(tomark);
            return new View(memtable, memtablesPendingFlush, sstables, compactingNew, intervalTree, sortedTokens);
        }
        
        public View unmarkCompacting(Iterable<SSTableReader> tounmark)
        {
        	ArrayListMultimap<Range, SSTableReader> map = ArrayListMultimap.<Range, SSTableReader>create();
        	for(SSTableReader sstable : tounmark)
        	{
        		map.put(sstable.getRange(), sstable);
        	}
        	return unmarkCompacting(map);
        }
        
        private View unmarkCompacting(ArrayListMultimap<Range, SSTableReader> tounmark)
        {
        	ArrayListMultimap<Range, SSTableReader> compactingNew = ArrayListMultimap.<Range, SSTableReader>create(compacting);
        	for(Map.Entry<Range, SSTableReader> entry : tounmark.entries())
        	{
        		compactingNew.remove(entry.getKey(), entry.getValue());
        	}
            return new View(memtable, memtablesPendingFlush, sstables, compactingNew, intervalTree, sortedTokens);
        }

		private ArrayListMultimap<Range,SSTableReader> newSSTables(
        		Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
        	ArrayListMultimap<Range, SSTableReader> old = ArrayListMultimap.<Range, SSTableReader>create();
        	ArrayListMultimap<Range, SSTableReader> replace = ArrayListMultimap.<Range, SSTableReader>create();
        	
        	for(SSTableReader sstable : oldSSTables)
        	{
        		old.put(sstable.getRange(), sstable);
        	}
        	for(SSTableReader sstable : replacements)
        	{
        		replace.put(sstable.getRange(), sstable);
        	}
        	
            return newSSTables(old, replace);
        }
        
        private ArrayListMultimap<Range,SSTableReader> newSSTables(Map<Range, SSTableReader> newSSTable)
        {
        	ArrayListMultimap<Range, SSTableReader> replacement = ArrayListMultimap.<Range, SSTableReader>create();
        	for(Map.Entry<Range, SSTableReader> entry : newSSTable.entrySet())
        	{
        		replacement.put(entry.getKey(), entry.getValue());
        	}
            return newSSTables(ArrayListMultimap.<Range, SSTableReader>create(), replacement);
        }

        private ArrayListMultimap<Range,SSTableReader> newSSTables(
        		ArrayListMultimap<Range, SSTableReader> oldSSTables, 
        		ArrayListMultimap<Range, SSTableReader> replacements)
        {
            ArrayListMultimap<Range, SSTableReader> newSSTables = ArrayListMultimap.<Range, SSTableReader>create();
            
            for (Range range : sstables.keySet())
            {
            	List<SSTableReader> olds = oldSSTables.get(range);
            	List<SSTableReader> currents = sstables.get(range);
            	for(SSTableReader sstable : currents)
            	{
            		if (!olds.contains(sstable))
                        newSSTables.put(range, sstable);
            	}
            }
            
            newSSTables.putAll(replacements);
            return newSSTables;
        }

        @Override
        public String toString()
        {
            return String.format("View(pending_count=%d, sstables=%s, compacting=%s)", memtablesPendingFlush.size(), sstables, compacting);
        }
    }
}
