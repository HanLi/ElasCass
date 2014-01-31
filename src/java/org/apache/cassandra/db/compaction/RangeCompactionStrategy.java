/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class RangeCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(RangeCompactionStrategy.class);
    protected static final long DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    
    protected static final String MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    protected static final long maxSSTableSize = DatabaseDescriptor.getMaximumSSTableSize();
    protected static final long maxConcurrentTask = 3;
    private static final int splitTaskPriority = 10;
    
    protected static long minSSTableSize;
    protected volatile int estimatedRemainingTasks;

    public RangeCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        String optionValue = options.get(MIN_SSTABLE_SIZE_KEY);
        minSSTableSize = (null != optionValue) ? Long.parseLong(optionValue) : DEFAULT_MIN_SSTABLE_SIZE;
        cfs.setMaximumCompactionThreshold(cfs.metadata.getMaxCompactionThreshold());
        cfs.setMinimumCompactionThreshold(cfs.metadata.getMinCompactionThreshold());
    }

    public List<AbstractCompactionTask> getBackgroundTasks(final int gcBefore)
    {
    	if(RangeCompactionTask.getConcurrentTaskCount() > maxConcurrentTask)
    		return Collections.<AbstractCompactionTask>emptyList();
    	
        if (cfs.isCompactionDisabled())
        {
            logger.debug("Compaction is currently disabled.");
            return Collections.<AbstractCompactionTask>emptyList();
        }
        
        List<AbstractCompactionTask> tasks = new LinkedList<AbstractCompactionTask>();
        Map<Pair<Range, Integer>, List<SSTableReader>> buckets = getBuckets(cfs.getSSTablesInBuckets(), minSSTableSize);
        List<Pair<Range, Integer>> priorityList = new ArrayList<Pair<Range, Integer>>(buckets.keySet());
        Collections.sort(priorityList, new Comparator<Pair<Range, Integer>>()
                {
                    public int compare(Pair<Range, Integer> o1, Pair<Range, Integer> o2)
                    {
                        return o1.right.compareTo(o2.right);
                    }
                });
        
        
        for (Pair<Range, Integer> pair : priorityList)
        {
        	if(RangeCompactionTask.getConcurrentTaskCount() + tasks.size() > maxConcurrentTask)
        		break;
        		
        	List<SSTableReader> sstables = buckets.get(pair);
            Collections.sort(sstables, new Comparator<SSTableReader>()
            {
                public int compare(SSTableReader o1, SSTableReader o2)
                {
                    return new Long(o1.bytesOnDisk()).compareTo(o2.bytesOnDisk());
                }
            });

            tasks.add(new RangeSplitTask(cfs, 
    				sstables.subList(0, Math.min(sstables.size(), cfs.getMaximumCompactionThreshold())), 
    				gcBefore, pair.left));
        }

        updateEstimatedCompactionsByTasks(tasks);
        return tasks;
    }

    public List<AbstractCompactionTask> getMaximalTasks(final int gcBefore)
    {
        List<AbstractCompactionTask> tasks = new LinkedList<AbstractCompactionTask>();
        Map<Range, Collection<SSTableReader>> buckets = cfs.getSSTablesInBuckets();
        
        for(Entry<Range, Collection<SSTableReader>> bucket : buckets.entrySet())
        {
        	if(!bucket.getValue().isEmpty())
        		tasks.add(new RangeCompactionTask(cfs, bucket.getValue(), gcBefore, bucket.getKey()));
        }
        return tasks;
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore)
    {
    	Range range = SSTableReader.validateAndGetRange(sstables);    	
        return new RangeCompactionTask(cfs, sstables, gcBefore, range)
                .isUserDefined(true)
                .compactionFileLocation(cfs.table.getDataFileLocation(1));
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    private static List<Pair<SSTableReader, Long>> createSSTableAndLengthPairs(Collection<SSTableReader> collection)
    {
        List<Pair<SSTableReader, Long>> tableLengthPairs = new ArrayList<Pair<SSTableReader, Long>>();
        for(SSTableReader sstable: collection)
            tableLengthPairs.add(new Pair<SSTableReader, Long>(sstable, sstable.onDiskLength()));
        return tableLengthPairs;
    }

    /**
     * Get sstables in buckets (grouped by token). Attempt to exclude large sstables in
     * the group.
     * 
     * @param ssTablesInBuckets
     * @param minSSTableSize
     * @return 
     */
    private Map<Pair<Range, Integer>, List<SSTableReader>> getBuckets(
    		Map<Range, Collection<SSTableReader>> ssTablesInBuckets, long minSSTableSize) 
    {
    	// Pair<Range, Integer> specifies the range and the priority of the task
    	Map<Pair<Range, Integer>, List<SSTableReader>> returnMap = Maps.newHashMapWithExpectedSize(ssTablesInBuckets.size());
		
    	for(Entry<Range, Collection<SSTableReader>> entry : ssTablesInBuckets.entrySet())
		{
			Range range = entry.getKey();
			if(!cfs.getDataTracker().isCompactable(range, entry.getValue()))
				continue;
			
			List<SSTableReader> sstables = new ArrayList<SSTableReader>(entry.getValue());
			
			if(StorageService.instance.isSplittable(cfs, range))
			{
				// smaller number == higher priority
				int priority = 0;
				if(sstables.size() <= cfs.getMinimumCompactionThreshold())
					priority = splitTaskPriority - sstables.size();
				
				Pair<Range, Integer> pair = new Pair<Range, Integer>(range, priority);
				returnMap.put(pair, sstables);
				continue;
			}
			
			List<Pair<SSTableReader, Long>> sstLengths = createSSTableAndLengthPairs(sstables);
			
			long totalSize = 0;
			for(Pair<SSTableReader, Long> pair : sstLengths)
			{
				totalSize += pair.right;
			}
			
			if(totalSize > maxSSTableSize)
			{
				// it is wrong to do it here. but that is the way it is.
				maybeInsertSplitTokenFor(range);
				
				Pair<Range, Integer> pair = new Pair<Range, Integer>(range, 1);
				returnMap.put(pair, sstables);
				continue;
			}
			
			// no need to split. Try to compact small files first.
			if(sstables.size() < cfs.getMinimumCompactionThreshold())
				continue;
			
			long sizeThreshold = Math.min(totalSize/6, minSSTableSize);
			
			// find sstables that are significantly small in the group
			List<SSTableReader> tiny = new ArrayList<SSTableReader>();
			SSTableReader tooLarge = null;
			
			for(Pair<SSTableReader, Long> pair : sstLengths)
			{
				if(pair.right < sizeThreshold)
					tiny.add(pair.left);
				if(pair.right > totalSize/4 * 3L)
					tooLarge = pair.left;
			}
			
			if(tiny.size() > 2 * cfs.getMinimumCompactionThreshold())
			{
				// do it when there is no split task
				int priority = Math.max(100 - tiny.size(), splitTaskPriority);
				Pair<Range, Integer> pair = new Pair<Range, Integer>(range, priority);
				returnMap.put(pair, tiny);
				continue;
			}
			
			if(tooLarge != null)
				sstables.remove(tooLarge);
				
			if(RangeCompactionTask.getConcurrentTaskCount()==0 && 
					sstables.size() >= cfs.getMinimumCompactionThreshold())
			{
				// do it when everything else is done
				// execute task with more sstables
				int priority = Math.max(200 - sstables.size(), splitTaskPriority);
				Pair<Range, Integer> pair = new Pair<Range, Integer>(range, priority);
				returnMap.put(pair, sstables);
			}
		}
		return returnMap;
	}

    private void maybeInsertSplitTokenFor(Range range)
    {
    	if(StorageService.instance.isSplittable(cfs, range))
    		return;
    	
    	Token token = RangeSplitTask.getSplitTokenFor(range, cfs.getSSTables(range));
    	assert range.contains(token);
    	// we know a new token will be inserted any way
    	StorageService.instance.addSplitToken(cfs, range, token);
    }
    
    private void updateEstimatedCompactionsByTasks(List<AbstractCompactionTask> tasks)
    {
        int n = 0;
        for (AbstractCompactionTask task: tasks)
        {
            if (!(task instanceof RangeCompactionTask))
                continue;

            Collection<SSTableReader> sstablesToBeCompacted = task.getSSTables();
            if (sstablesToBeCompacted.size() >= cfs.getMinimumCompactionThreshold())
                n += Math.ceil((double)sstablesToBeCompacted.size() / cfs.getMaximumCompactionThreshold());
        }
        estimatedRemainingTasks = n;
    }

    public long getMinSSTableSize()
    {
        return minSSTableSize;
    }

    public long getMaxSSTableSize()
    {
        return maxSSTableSize;
    }

    public boolean isKeyExistenceExpensive(Set<? extends SSTable> sstablesToIgnore)
    {
        return false;
    }

    public String toString()
    {
        return String.format("RangeCompactionStrategy[%s/%s]",
            cfs.getMinimumCompactionThreshold(),
            cfs.getMaximumCompactionThreshold());
    }
}
