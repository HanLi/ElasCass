package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

public class RangeSplitTask extends RangeCompactionTask {
	
	protected static final Logger logger = LoggerFactory.getLogger(RangeSplitTask.class);
	
	private static final Token initToken = StorageService.getPartitioner().getToken(SystemTable.TOKEN);
	
	
	public RangeSplitTask(ColumnFamilyStore cfs,
			Collection<SSTableReader> sstables, int gcBefore, Range range) 
	{
		super(cfs, sstables, gcBefore, range);
	}

	@Override
    public int execute(CompactionManager.CompactionExecutorStatsCollector collector) throws IOException
    {
    	concurrentTasks.incrementAndGet();
    	try {
    		return executeInternal(collector);
    	}
    	finally
    	{
    		concurrentTasks.decrementAndGet();
    	}
    }
	
	
	public int executeInternal(CompactionManager.CompactionExecutorStatsCollector collector) throws IOException
	{
		assert sstables != null;
		
		List<SSTableReader> toCompact = new ArrayList<SSTableReader>(sstables);
		
		if (compactionFileLocation == null)
            compactionFileLocation = cfs.table.getDataFileLocation(cfs.getExpectedCompactedFileSize(toCompact));
		if (compactionFileLocation == null)
        {
            logger.warn("insufficient space to compact even the two smallest files, aborting");
            return 0;
        }

        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            cfs.snapshotWithoutFlush(System.currentTimeMillis() + "-" + "compact-" + cfs.columnFamily);

        // sanity check: all sstables must belong to the same cfs
        for (SSTableReader sstable : toCompact)
            assert sstable.descriptor.cfname.equals(cfs.columnFamily);
		
        CompactionController controller = new CompactionController(cfs, toCompact, gcBefore, isUserDefined);
        // new sstables from flush can be added during a compaction, but only the compaction can remove them,
        // so in our single-threaded compaction world this is a valid way of determining if we're compacting
        // all the sstables (that existed when we started)
        logger.info("Compacting {}", toCompact);

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        long estimatedTotalKeys = Math.max(DatabaseDescriptor.getIndexInterval(), SSTableReader.getApproximateKeyCount(toCompact));
        long estimatedSSTables = Math.max(1, SSTable.getTotalBytes(toCompact) / cfs.getCompactionStrategy().getMaxSSTableSize());
        long keysPerSSTable = (long) Math.ceil((double) estimatedTotalKeys / estimatedSSTables);
        if (logger.isDebugEnabled())
            logger.debug("Expected bloom filter size : " + keysPerSSTable);

        AbstractCompactionIterable ci = DatabaseDescriptor.isMultithreadedCompaction()
                                      ? new ParallelCompactionIterable(OperationType.COMPACTION, toCompact, controller)
                                      : new CompactionIterable(OperationType.COMPACTION, toCompact, controller);
        CloseableIterator<AbstractCompactedRow> iter = ci.iterator();
        Iterator<AbstractCompactedRow> nni = Iterators.filter(iter, Predicates.notNull());
        Map<DecoratedKey, Long> cachedKeys = new HashMap<DecoratedKey, Long>();

        // we can't preheat until the tracker has been set. This doesn't happen until we tell the cfs to
        // replace the old entries.  Track entries to preheat here until then.
        Map<SSTableReader, Map<DecoratedKey, Long>> cachedKeyMap =  new HashMap<SSTableReader, Map<DecoratedKey, Long>>();

        Collection<SSTableReader> sstables = new ArrayList<SSTableReader>();
        Collection<SSTableWriter> writers = new ArrayList<SSTableWriter>();

        if (collector != null)
            collector.beginCompaction(ci);
        try
        {
            if (!nni.hasNext())
            {
                // don't mark compacted in the finally block, since if there _is_ nondeleted data,
                // we need to sync it (via closeAndOpen) first, so there is no period during which
                // a crash could cause data loss.
                cfs.markCompacted(toCompact);
                return 0;
            }

            Token splitToken = StorageService.instance.getSuccessorTokenFor(cfs, range.left);
    		Range currentRange = new Range(range.left, splitToken);
            SSTableWriter writer = cfs.createCompactionWriter(
            		currentRange, keysPerSSTable, compactionFileLocation, toCompact);
            writers.add(writer);
            
            AbstractCompactedRow row = null;
            while (nni.hasNext())
            {
                row = nni.next();
                if (row.isEmpty())
                    continue;

                if(!currentRange.contains(row.key.token))
                {
                	// reach the upper bound
//                	assert !currentRange.right.equals(range.right);
//                	if(currentRange.right.equals(range.right))
//                	{
//                		throw new RuntimeException("found row " + row + " which does not belong to Range " + range);
//                	}
                	
                	if(writer.first==null)
                	{
                		writer.abort();
                	}
                	else
                	{
                		SSTableReader toIndex = writer.closeAndOpenReader(getMaxDataAge(toCompact));
                        cachedKeyMap.put(toIndex, cachedKeys);
                        sstables.add(toIndex);
                	}
                	
                	do {
                		splitToken = StorageService.instance.getSuccessorTokenFor(cfs, currentRange.right);
                        currentRange = new Range(currentRange.right, splitToken);
                	} 
                	while (!currentRange.contains(row.key.token));
                	
                    writer = cfs.createCompactionWriter(currentRange, keysPerSSTable, compactionFileLocation, toCompact);
                    writers.add(writer);
                    cachedKeys = new HashMap<DecoratedKey, Long>();
                }
                
                long position = writer.append(row);
                totalkeysWritten++;

                if (DatabaseDescriptor.getPreheatKeyCache())
                {
                    for (SSTableReader sstable : toCompact)
                    {
                        if (sstable.getCachedPosition(row.key, false) != null)
                        {
                            cachedKeys.put(row.key, position);
                            break;
                        }
                    }
                }
                if (newSSTableSegmentThresholdReached(writer, position) && nni.hasNext())
                {
                    SSTableReader toIndex = writer.closeAndOpenReader(getMaxDataAge(toCompact));
                    cachedKeyMap.put(toIndex, cachedKeys);
                    sstables.add(toIndex);
                    
                    writer = cfs.createCompactionWriter(currentRange, keysPerSSTable, compactionFileLocation, toCompact);
                    writers.add(writer);
                    cachedKeys = new HashMap<DecoratedKey, Long>();
                }
            }
            
            SSTableReader toIndex = writer.closeAndOpenReader(getMaxDataAge(toCompact));
            cachedKeyMap.put(toIndex, cachedKeys);
            sstables.add(toIndex);

        }
        catch (Exception e)
        {
            for (SSTableWriter writer : writers)
                writer.abort();
            throw FBUtilities.unchecked(e);
        }
        finally
        {
            iter.close();
            if (collector != null)
                collector.finishCompaction(ci);
        }

        cfs.replaceCompactedSSTables(toCompact, sstables);
        
        // TODO: this doesn't belong here, it should be part of the reader to load when the tracker is wired up
        for (Entry<SSTableReader, Map<DecoratedKey, Long>> ssTableReaderMapEntry : cachedKeyMap.entrySet())
        {
            SSTableReader key = ssTableReaderMapEntry.getKey();
            for (Entry<DecoratedKey, Long> entry : ssTableReaderMapEntry.getValue().entrySet())
               key.cacheKey(entry.getKey(), entry.getValue());
        }
        
        long dTime = System.currentTimeMillis() - startTime;
        long startsize = SSTable.getTotalBytes(toCompact);
        long endsize = SSTable.getTotalBytes(sstables);
        double ratio = (double)endsize / (double)startsize;

        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (SSTableReader reader : sstables)
            builder.append(reader.getFilename()).append(",");
        builder.append("]");

        double mbps = dTime > 0 ? (double)endsize/(1024*1024)/((double)dTime/1000) : 0;
        logger.info(String.format("Compacted to %s.  %,d to %,d (~%d%% of original) bytes for %,d keys at %fMB/s.  Time: %,dms.",
                                  builder.toString(), startsize, endsize, (int) (ratio * 100), totalkeysWritten, mbps, dTime));
        logger.debug(String.format("CF Total Bytes Compacted: %,d", CompactionTask.addToTotalBytesCompacted(endsize)));
        return toCompact.size();
	}
	
	public static Token getSplitTokenFor(Range range, Collection<SSTableReader> sstables) 
	{
		Set<SSTableReader> sstableSet = Sets.newHashSet(sstables);

		Token token;
		if(sstableSet.size() > 3 && !range.left.equals(initToken))
		{
			List<Pair<Token, Double>> list = createMidTokenAndWeightPair(sstableSet);
			token = getWeightedMidTokenFor(list);
		}
		else
		{
			Token maxFirst = null, minLast = null;

			for(SSTableReader sstable : sstableSet)
			{
				if(maxFirst == null || maxFirst.compareTo(sstable.first.token) < 0)
					maxFirst = sstable.first.token;

				if(minLast == null || minLast.compareTo(sstable.last.token) > 0)
					minLast = sstable.last.token;
			}

			assert maxFirst != null;
			assert minLast != null;

			if(maxFirst.compareTo(minLast) > 0)
			{
				token = getMidToken(minLast, maxFirst, StorageService.getPartitioner());
			}
			else
			{
				token = getMidToken(maxFirst, minLast, StorageService.getPartitioner());
			}
			
		}
		
		logger.info("hli@cse\tgetSplitTokenFor with " + token + " for " + range + " {}", sstableSet);
		
		return token;
	}
	
//	public static Token getSplitTokenFor(Iterable<SSTableReader> sstables) 
//	{
//		assert sstables.iterator().hasNext();
//		Token min = null, max = null;
//
//		for(SSTableReader sstable : sstables)
//		{
//			if(min == null || min.compareTo(sstable.first.token) > 0)
//				min = sstable.first.token;
//			
//			if(max == null || max.compareTo(sstable.last.token) < 0)
//				max = sstable.last.token;
//		}
//		
//		assert min != null;
//		assert max != null;
//		
//		return getMidToken(min, max, StorageService.getPartitioner());
//	}
	
	static Token getWeightedMidTokenFor(List<Pair<Token, Double>> list)
	{	
		IPartitioner partitioner = StorageService.getPartitioner();
		
		assert !list.isEmpty();
		
		while(list.size() > 1)
		{
			Collections.sort(list, new Comparator<Pair<Token, Double>>()
					{
				@Override
				public int compare(Pair<Token, Double> o1, Pair<Token, Double> o2) 
				{
					// make it descendent
					return o2.right.compareTo(o1.right);
				}});
			
			Pair<Token, Double> small = list.remove(list.size()-1);
			Pair<Token, Double> large = list.remove(list.size()-1);
			
			double ratio = large.right / small.right;
			Pair<Token, Double> newPair;
			
			if(ratio > 11.0) //btw 1/7 and 1/15
			{
				newPair = new Pair<Token, Double>(large.left, small.right + large.right); // absorb
			}
			else if(ratio > 5.0) //btw 1/7 and 1/3
			{
				Token midToken = getMidToken(large.left, small.left, partitioner); // 0.5
				midToken = getMidToken(large.left, midToken, partitioner); // 0.75
				midToken = getMidToken(large.left, midToken, partitioner); // 0.875
				newPair = new Pair<Token, Double>(midToken, small.right + large.right);
			}
			else if(ratio > 2.0) //btw 1/1 and 1/3
			{
				Token midToken = getMidToken(large.left, small.left, partitioner); // 0.5
				midToken = getMidToken(large.left, midToken, partitioner); // 0.75
				newPair = new Pair<Token, Double>(midToken, small.right + large.right);
			}
			else
			{
				Token midToken = getMidToken(large.left, small.left, partitioner); // 0.5
				newPair = new Pair<Token, Double>(midToken, small.right + large.right);
			}

			list.add(newPair);
		}
		
		return list.get(0).left;
	}
	
	private static Token getMidToken(Token t1, Token t2, IPartitioner partitioner)
	{
		if(t1.compareTo(t2) > 0)
			return partitioner.midpoint(t2, t1);
		
		return partitioner.midpoint(t1, t2);
	}
	
	private static List<Pair<Token, Double>> createMidTokenAndWeightPair(Collection<SSTableReader> sstables)
	{
		IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
		List<Pair<Token, Double>> list = new ArrayList<Pair<Token, Double>>();

		for(SSTableReader sstable : sstables)
		{
			Token mid = partitioner.midpoint(sstable.first.token, sstable.last.token);
			Pair<Token, Double> pair = new Pair<Token, Double>(mid, (double)sstable.bytesOnDisk());
			list.add(pair);
		}
		
		return list;
	}
	
}
