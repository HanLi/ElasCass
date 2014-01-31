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

package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.DataVolumeRequester;
import org.apache.cassandra.service.LoadRequester;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.streaming.StreamIn;
import org.apache.cassandra.streaming.StreamOut;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SimpleCondition;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;


public class BootStrapper
{
	private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);

	//    private static final Map<Pair<String, String>, Multimap<InetAddress, Range>> rangesToFetch = 
	//    	new HashMap<Pair<String, String>, Multimap<InetAddress, Range>>();

	private static final Map<Pair<String, String>, Multimap<Range, InetAddress>> rangesToStream = 
		new HashMap<Pair<String, String>, Multimap<Range, InetAddress>>();

	private static final long BOOTSTRAP_TIMEOUT = 30000; // default bootstrap timeout of 30s

	public static void bootstrap(String tableName, String cfName, Collection<Token> tokens)
	{
		if (logger.isDebugEnabled())
			logger.debug("Beginning bootstrap process");

		TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata(tableName, cfName);
		// get cpu usage of each node
		Set<InetAddress> allHosts = tokenMetadata.getNormalEndpoints().keySet();
		Map<InetAddress, Integer> numTasks = Maps.newHashMap();
		for(InetAddress host : allHosts)
		{
			numTasks.put(host, 0);
		}

		Multimap<Range, InetAddress> myRangeAddresses = ArrayListMultimap.create();
		for(Token token : tokens)
		{
			Set<InetAddress> eps = tokenMetadata.getEndpoints(token);
			List<InetAddress> preferred = getSortedListByTaskNum(eps, numTasks);
			Range range = tokenMetadata.getPrimaryRangeFor(token);
			myRangeAddresses.putAll(range, preferred);
		}

		Multimap<InetAddress, Range> workMap = getWorkMap(myRangeAddresses);
		requestRanges(tableName, cfName, workMap);
	}

	public static void unbootstrap(String tableName, ColumnFamilyStore cfs)
	{
		logger.info("Start streaming data of " + tableName + "-" + cfs.columnFamily + " to other nodes");
		//    	throw new UnsupportedOperationException("Not Implemented");
		//    	
		//        if (logger.isDebugEnabled())
		//            logger.debug("Beginning bootstrap process");
		//        
		//        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata(tableName, cfName);
		//
		//        Set<Range> ranges = tokenMetadata.getPrimaryRangesFor(FBUtilities.getBroadcastAddress());
		//        Set<InetAddress> allEndpoints = tokenMetadata.getNormalEndpoints().keySet();
		//        
		//        ColumnFamilyStore cfs = Table.open(tableName).getColumnFamilyStore(cfName);
		//        AbstractReplicationStrategy strategy = cfs.getReplicationStrategy();
		//        
		//        Multimap<Range, InetAddress> myRangeAddresses = HashMultimap.create();
		//        for(Range range : ranges)
		//        {
		//        	List<InetAddress> currentEndpoints = strategy.calculateNaturalEndpoints(range.right, tokenMetadata);
		//        	if(currentEndpoints.size() <= strategy.getReplicationFactor()
		//        			&& currentEndpoints.size() < allEndpoints.size())
		//        	{
		//        		Set<InetAddress> endpoints = new HashSet<InetAddress>(allEndpoints);
		//        		endpoints.removeAll(currentEndpoints);
		//        		List<InetAddress> newEndpoints = getPreferredEndpointsForMigration(endpoints, 
		//        				DataVolumeBroadcaster.instance.getVolumeInfo(cfs.metadata.cfId));
		//        		int numNewEndpoints = Math.min(newEndpoints.size(), 
		//    					strategy.getReplicationFactor() - currentEndpoints.size() - 1);
		//        		
		//        		myRangeAddresses.putAll(range, newEndpoints.subList(0, numNewEndpoints));
		//        	}
		//        }
		//        
		//        Pair<String, String> columnfamily = new Pair<String, String>(tableName, cfName);
		//        rangesToStream.put(columnfamily, myRangeAddresses);
	}

	/**
	 * Gets two lists of tokens. One for bootstrapping hot tokens, the other for balanced data amount.
	 * @param keyspace
	 * @param columnFamily
	 * @param metadata
	 * @param strategy
	 * @return
	 */
	public static Pair<List<Token>, List<Token>> getTokensForBalancedLoad(String keyspace, String columnFamily, 
			TokenMetadata metadata, AbstractReplicationStrategy strategy)
	{
		List<Token> retHot = Lists.newArrayList();
		List<Token> retCold = Lists.newArrayList();

		Set<InetAddress> hosts = Sets.newHashSet(metadata.getNormalEndpoints().keySet());

		int repFactor = strategy.getReplicationFactor();
		double sumReplicas = 0;
		Map<Token, Collection<InetAddress>> map = metadata.getTokenToEndpointsMapForReading();

		// satisfy the replication level
		for(Map.Entry<Token, Collection<InetAddress>> entry : map.entrySet())
		{
			int replicas = entry.getValue().size();
			sumReplicas += replicas;

			if(replicas < repFactor)
			{
				retHot.add(entry.getKey());
			}
		}

		int need = (int) Math.floor(sumReplicas / (hosts.size()+1));
		if(retHot.size() >= need)
			return new Pair<List<Token>, List<Token>>(retHot, retCold);

		// get cpu usage of each node
		Map<InetAddress, Double> systemLoads = LoadRequester.instance.getSystemCpuUsage(hosts);
		// get loads for each token
		List<InetAddress> priorList = getPriorBootstrapSourcesByLoad(systemLoads);
		Map<Token, Pair<AtomicInteger, AtomicInteger>> loadStats = Maps.newHashMap();
		Map<InetAddress, Map<Token, Integer>> hotTokens = Maps.newHashMap();

		for(int i=0; i<priorList.size(); i++)
		{
			InetAddress endpoint = priorList.get(i);
			Map<Token, Integer> tokens = LoadRequester.instance.getHotSpotTokens(
					endpoint, keyspace, columnFamily);

			hotTokens.put(endpoint, tokens);

			for(Map.Entry<Token, Integer> entry : tokens.entrySet())
			{
				logger.info("From " + endpoint + " Hit Count for " + entry.getKey() + " is " + entry.getValue());

				Token token = entry.getKey();
				if(retHot.contains(token))
					continue;

				if(loadStats.containsKey(token))
				{
					Pair<AtomicInteger,AtomicInteger> stat = loadStats.get(token);
					stat.left.addAndGet(entry.getValue());
					stat.right.incrementAndGet();
				}
				else
				{
					Pair<AtomicInteger,AtomicInteger> stat = new Pair<AtomicInteger,AtomicInteger>(
							new AtomicInteger(entry.getValue()), new AtomicInteger(1));
					loadStats.put(token, stat);
				}
			}
		}

		// get the hot tokens
		int thres = (int) (getAverageLoad(loadStats) * DatabaseDescriptor.getHotSpotRatio());
		
		for(Token token : retHot)
		{
			loadStats.remove(token);
		}
		List<Token> rankedHotTokens = getPriorHotTokens(loadStats);
		
		int hotTokenNum = 0;

		for(Token token : rankedHotTokens)
		{
			Pair<AtomicInteger,AtomicInteger> stat = loadStats.get(token);
			int load = stat.left.intValue() / stat.right.intValue();
			if(load >= thres)  // follows the 80/20 proportion
			{
				retHot.add(token);
				loadStats.remove(token);
				hotTokenNum++;
				
				if(retHot.size() >= need)
					return new Pair<List<Token>, List<Token>>(retHot, retCold);
			}
			else
				break;
		}

		// max num of tokens from each node
		int avgReplicas = (int) Math.ceil((double)(need-retHot.size()) / hosts.size());

		for(InetAddress ep : priorList)
		{
			Map<Token, Integer> epLoads = hotTokens.get(ep);
			List<Token> epHotToken;
			if(hotTokenNum <= 2*need/3)
				epHotToken = getPriorHotTokens(epLoads, true);
			else
				epHotToken = getPriorHotTokens(epLoads, false);

			int count = 0;
			for(Token token : epHotToken)
			{
				if(!retHot.contains(token) && !retCold.contains(token))
				{
					retCold.add(token);
					hotTokenNum++;
					
					if(retHot.size()+retCold.size() >= need)
						return new Pair<List<Token>, List<Token>>(retHot, retCold);

					if(++count > avgReplicas)
						break;
				}
			}
		}

		return new Pair<List<Token>, List<Token>>(retHot, retCold);
	}

	/**
	 * 
	 * @param keyspace
	 * @param columnFamily
	 * @param metadata
	 * @param strategy
	 * @return a mapping between the token and the endpoint that should hand over the token. 
	 * Use local address to indicate that no endpoint should give away the token.
	 */
	public static Map<Token, InetAddress> getTokensForBalancedData(String keyspace, String columnFamily, 
			TokenMetadata metadata, AbstractReplicationStrategy strategy) 
	{
		Map<Token, InetAddress> retTokens = Maps.newHashMap();

		Map<InetAddress, Long> volumes = DataVolumeRequester.instance.getVolumeInfo(
				keyspace, columnFamily, metadata.getNormalEndpoints().keySet());

		int repFactor = strategy.getReplicationFactor();
		double sumReplicas = 0;
		Map<Token, Collection<InetAddress>> map = metadata.getTokenToEndpointsMapForReading();

		for(Map.Entry<Token, Collection<InetAddress>> entry : map.entrySet())
		{
			int replicas = entry.getValue().size();
			sumReplicas += replicas;

			if(replicas < repFactor)
			{
				retTokens.put(entry.getKey(), FBUtilities.getBroadcastAddress());
			}
		}

		int avgReplicas = (int) Math.ceil(sumReplicas / (volumes.size()+1));
		int need = (int) Math.floor(sumReplicas / (volumes.size()+1));

		if(retTokens.size() >= need)
			return retTokens;

		List<InetAddress> list = getPriorBootstrapSourcesByVolume(volumes);
		for(int i=0; i< list.size(); i++)
		{
			InetAddress endpoint = list.get(i);
			Set<Token> tokenSet = metadata.getTokens(endpoint);
			SetView<Token> remains = Sets.difference(tokenSet, retTokens.keySet());
			int canOffer = tokenSet.size() - avgReplicas;

			for(Token token : remains)
			{
				if(canOffer <= 0)
					break;

				if(!retTokens.containsKey(token))
				{
					retTokens.put(token, endpoint);
					if(retTokens.size() >= need)
						return retTokens;

					canOffer--;
				}
			}
		}
		return retTokens;
	}
	

	public static synchronized void requestRanges(final String tableName, final String cfName, 
			Multimap<InetAddress, Range> workMap)
	{
		final CountDownLatch latch = new CountDownLatch(workMap.keySet().size());
		/* Send messages to respective folks to stream data over to me */
		for (final InetAddress source : workMap.keySet())
		{
			final Collection<Range> ranges = workMap.get(source);

			final Runnable callback = new Runnable()
			{
				public void run()
				{
					latch.countDown();
					if (logger.isDebugEnabled())
						logger.debug(String.format("Finished requesting (%s-%s) from %s; remaining is %s",
						tableName, cfName, source.getHostAddress(), latch.getCount()));
				}
			};
			logger.info("Requesting data from " + source + " for " 
					+ ranges.size() + " ranges " + StringUtils.join(ranges, ",")); 
			StreamIn.requestRanges(source, tableName, cfName, ranges, callback, OperationType.BOOTSTRAP);
		}

		try
		{
			latch.await();
		}
		catch (InterruptedException e)
		{
			throw new AssertionError(e);
		}
	}

	public static synchronized void streamAllRanges()
	{
		final CountDownLatch latch = new CountDownLatch(rangesToStream.keySet().size());
		for (final Pair<String, String> pair : rangesToStream.keySet())
		{
			final ColumnFamilyStore cfs = Table.open(pair.left).getColumnFamilyStore(pair.right);

			final Multimap<Range, InetAddress> rangeAddresses = rangesToStream.get(pair);
			if(rangeAddresses.isEmpty())
			{
				latch.countDown();
				continue;
			}

			final Set<Entry<Range, InetAddress>> pendings = new HashSet<Entry<Range, InetAddress>>(rangeAddresses.entries());
			/* Send messages to respective folks to stream data over to me */
			for (final Entry<Range, InetAddress> entry : rangeAddresses.entries())
			{
				final Runnable callback = new Runnable()
				{
					public void run()
					{
						pendings.remove(entry);
						if(pendings.isEmpty())
						{
							latch.countDown();
							if (logger.isDebugEnabled())
								logger.debug(String.format("Finished streaming (%s-%s); remaining is %s",
								cfs.table.name, cfs.columnFamily, latch.getCount()));
						}
					}
				};
				if (logger.isDebugEnabled())
					logger.debug("Streaming to " + entry.getValue() + " range " + entry.getKey());

				StageManager.getStage(Stage.STREAM).execute(new Runnable()
				{
					public void run()
					{
						// TODO each call to transferRanges re-flushes, this is potentially a lot of waste
						StreamOut.transferRanges(entry.getValue(), cfs, Arrays.asList(entry.getKey()), callback, OperationType.UNBOOTSTRAP);
					}
				});
			}
		}

		try
		{
			latch.await();
			rangesToStream.clear();
		}
		catch (InterruptedException e)
		{
			throw new AssertionError(e);
		}
	}

	public static Boolean removeTokens(String keyspace, String columnfamily, 
			InetAddress endpoint, Set<Token> tokenSet)
	{
		TokenFactory tf = StorageService.instance.getPartitioner().getTokenFactory();
		String body = keyspace + "\t" + columnfamily + "\t" + tf.toString(tokenSet);
		long timeout = Math.max(MessagingService.getDefaultCallbackTimeout(), BOOTSTRAP_TIMEOUT);

		Message message = new Message(FBUtilities.getBroadcastAddress(),
				StorageService.Verb.REMOVE_TOKEN,
				body.getBytes(Charsets.UTF_8),
				Gossiper.instance.getVersion(endpoint));

		RemoveTokenCallback rtc = new RemoveTokenCallback();
		MessagingService.instance().sendRR(message, endpoint, rtc, timeout);
		return rtc.getResponse(timeout);
	}

	public static Boolean removingTokens(String keyspace, String columnfamily,
			InetAddress endpoint, Set<Token> tokenSet) 
	{
		TokenFactory tf = StorageService.instance.getPartitioner().getTokenFactory();
		String body = keyspace + "\t" + columnfamily + "\t" + tf.toString(tokenSet);
		long timeout = Math.max(MessagingService.getDefaultCallbackTimeout(), BOOTSTRAP_TIMEOUT);

		Message message = new Message(FBUtilities.getBroadcastAddress(),
				StorageService.Verb.REMOVING_TOKEN,
				body.getBytes(Charsets.UTF_8),
				Gossiper.instance.getVersion(endpoint));

		RemovingTokenCallback rtc = new RemovingTokenCallback();
		MessagingService.instance().sendRR(message, endpoint, rtc, timeout);
		return rtc.getResponse(timeout);
	}

	public static Map<InetAddress, Collection<Token>> getRingStateFrom(
			String keyspace, String columnFamily, Iterable<InetAddress> seedProviders) 
			{
		String body = keyspace + "\t" + columnFamily;
		long timeout = Math.max(MessagingService.getDefaultCallbackTimeout(), BOOTSTRAP_TIMEOUT);

		Map<InetAddress, Collection<Token>> ring = null;
		for(InetAddress endpoint : seedProviders)
		{
			Message message = new Message(FBUtilities.getBroadcastAddress(),
					StorageService.Verb.RING,
					body.getBytes(Charsets.UTF_8),
					Gossiper.instance.getVersion(endpoint));

			RingStateCallback rsc = new RingStateCallback();
			MessagingService.instance().sendRR(message, endpoint, rsc, timeout);
			ring = rsc.getRing(timeout);
			if (ring!=null && ring.isEmpty()==false)
				return ring;
		}
		return ring;
			}

	public static Collection<Token> getTokens(TokenMetadata metadata) 
	{
		Map<Token, Collection<InetAddress>> tokenToEndpoints = metadata.getTokenToEndpointsMapForReading();

		int minSize = Integer.MAX_VALUE;
		Token token = null;
		for(Entry<Token, Collection<InetAddress>> entry : tokenToEndpoints.entrySet())
		{
			if(entry.getValue().size() < minSize)
			{
				minSize = entry.getValue().size();
				token = entry.getKey();
			}
		}
		return Collections.singleton(token);
	}

	private static int getAverageLoad(Map<Token, Pair<AtomicInteger, AtomicInteger>> loadStats) 
	{
		if(loadStats.isEmpty())
			return 0;

		int sum = 0;
		for(Pair<AtomicInteger, AtomicInteger> stat : loadStats.values())
		{
			sum += stat.left.intValue() / stat.right.intValue();
		}

		return sum/loadStats.size();
	}

	private static List<Token> getPriorHotTokens(
			final Map<Token, Pair<AtomicInteger, AtomicInteger>> loadStats) 
	{
		if(loadStats.isEmpty())
			return Lists.newArrayList();

		List<Token> list = Lists.newArrayList(loadStats.keySet());

		// order by desc
		Collections.sort(list, new Comparator<Token>()
				{
			public int compare(Token t1, Token t2)
			{
				Pair<AtomicInteger,AtomicInteger> stat1 = loadStats.get(t1);
				Pair<AtomicInteger,AtomicInteger> stat2 = loadStats.get(t2);
				
				int load1 = stat1.left.intValue() / stat1.right.intValue();
				int load2 = stat2.left.intValue() / stat2.right.intValue();
				return load2 - load1;  // desc order
			}
				});

		return list;
	}
	
	
	private static List<Token> getPriorHotTokens(final Map<Token, Integer> loads, boolean desc) 
	{
		if(loads.isEmpty())
			return Lists.newArrayList();

		List<Token> list = Lists.newArrayList(loads.keySet());

		// order by desc
		if(desc)
		{
			Collections.sort(list, new Comparator<Token>()
					{
				public int compare(Token t1, Token t2)
				{
					Integer load1 = loads.get(t1);
					if(load1 == null)
						load1 = 0;
					Integer load2 = loads.get(t2);
					if(load2 == null)
						load2 = 0;
					
					return load2.compareTo(load1);  // desc order
				}
					});
		}
		else
		{
			Collections.sort(list, new Comparator<Token>()
					{
				public int compare(Token t1, Token t2)
				{
					Integer load1 = loads.get(t1);
					if(load1 == null)
						load1 = 0;
					Integer load2 = loads.get(t2);
					if(load2 == null)
						load2 = 0;
					
					return load1.compareTo(load2);
				}
					});
		}

		return list;
	}
	

	public static Token getTokenForSplitting(InetAddress endpoint, 
			String keyspace, String columnFamily, Range range, Token token)
	{
		TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
		String left = tf.toString(range.left);
		String right = tf.toString(range.right);
		String tokenStr = tf.toString(token);

		String body = keyspace + "\t" + columnFamily + "\t" + left + "\t" + right + "\t" + tokenStr;
		long timeout = Math.max(MessagingService.getDefaultCallbackTimeout(), BOOTSTRAP_TIMEOUT);

		Message message = new Message(FBUtilities.getBroadcastAddress(),
				StorageService.Verb.SPLIT,
				body.getBytes(Charsets.UTF_8),
				Gossiper.instance.getVersion(endpoint));

		Token retToken = null;
		for(int i=0; i<3; i++)
		{
			logger.info("hli@cse\tAttempting to get split token from " + endpoint.getHostAddress() 
					+ " for Range " + range);
			SplitRangeCallback src = new SplitRangeCallback();
			MessagingService.instance().sendRR(message, endpoint, src, timeout);
			retToken = src.getToken(timeout);
			if(retToken != null)
				return retToken;

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		assert retToken != null;
		return retToken;
	}


	static List<InetAddress> getPriorBootstrapSourcesByVolume(final Map<InetAddress, Long> volumes)
	{
		List<InetAddress> endpoints = new ArrayList<InetAddress>(volumes.size());
		for(InetAddress endpoint : volumes.keySet())
		{
			endpoints.add(endpoint);
		}

		if(endpoints.isEmpty())
			throw new RuntimeException("No other nodes seen!  Unable to bootstrap."
					+ "If you intended to start a single-node cluster, you should make sure "
					+ "your broadcast_address (or listen_address) is listed as a seed.  "
					+ "Otherwise, you need to determine why the seed being contacted "
					+ "has no knowledge of the rest of the cluster.  Usually, this can be solved "
					+ "by giving all nodes the same seed list.");

		Collections.sort(endpoints, new Comparator<InetAddress>()
				{
			public int compare(InetAddress ia1, InetAddress ia2)
			{
				Long bytes1 = volumes.get(ia1);
				Long bytes2 = volumes.get(ia2);
				return bytes2.compareTo(bytes1); // more bytes == higher priority
			}
				});

		return endpoints;
	}

	public static List<InetAddress> getPriorBootstrapSourcesByLoad(final Map<InetAddress, Double> load)
	{
		List<InetAddress> endpoints = Lists.newArrayList(load.keySet());

		if(endpoints.isEmpty())
			throw new RuntimeException("No other nodes seen!  Unable to bootstrap."
					+ "If you intended to start a single-node cluster, you should make sure "
					+ "your broadcast_address (or listen_address) is listed as a seed.  "
					+ "Otherwise, you need to determine why the seed being contacted "
					+ "has no knowledge of the rest of the cluster.  Usually, this can be solved "
					+ "by giving all nodes the same seed list.");

		Collections.sort(endpoints, new Comparator<InetAddress>()
				{
			public int compare(InetAddress ia1, InetAddress ia2)
			{
				Double load1 = load.get(ia1);
				Double load2 = load.get(ia2);
				return load2.compareTo(load1);  // desc order
			}
				});

		return endpoints;
	}

	//    static InetAddress getBootstrapSource(final TokenMetadata metadata, final Map<InetAddress, Double> load)
	//    {
	//        // sort first by number of nodes already bootstrapping into a source node's range, then by load.
	//        List<InetAddress> endpoints = new ArrayList<InetAddress>(load.size());
	//        for (InetAddress endpoint : load.keySet())
	//        {
	//            if (!metadata.isMember(endpoint))
	//                continue;
	//            endpoints.add(endpoint);
	//        }
	//
	//        if (endpoints.isEmpty())
	//            throw new RuntimeException("No other nodes seen!  Unable to bootstrap."
	//                                       + "If you intended to start a single-node cluster, you should make sure "
	//                                       + "your broadcast_address (or listen_address) is listed as a seed.  "
	//                                       + "Otherwise, you need to determine why the seed being contacted "
	//                                       + "has no knowledge of the rest of the cluster.  Usually, this can be solved "
	//                                       + "by giving all nodes the same seed list.");
	//        Collections.sort(endpoints, new Comparator<InetAddress>()
	//        {
	//            public int compare(InetAddress ia1, InetAddress ia2)
	//            {
	//                int n1 = metadata.pendingRangeChanges(ia1);
	//                int n2 = metadata.pendingRangeChanges(ia2);
	//                if (n1 != n2)
	//                    return -(n1 - n2); // more targets = _less_ priority!
	//
	//                double load1 = load.get(ia1);
	//                double load2 = load.get(ia2);
	//                if (load1 == load2)
	//                    return 0;
	//                return load1 < load2 ? -1 : 1;
	//            }
	//        });
	//
	//        InetAddress maxEndpoint = endpoints.get(endpoints.size() - 1);
	//        assert !maxEndpoint.equals(FBUtilities.getBroadcastAddress());
	//        if (metadata.pendingRangeChanges(maxEndpoint) > 0)
	//            throw new RuntimeException("Every node is a bootstrap source! Please specify an initial token manually or wait for an existing bootstrap operation to finish.");
	//        
	//        return maxEndpoint;
	//    }

	//    /** get potential sources for each range, ordered by proximity (as determined by EndpointSnitch) */
	//    Multimap<Range, InetAddress> getRangesWithSources(String table)
	//    {
	//        assert tokenMetadata.getSortedTokens().size() > 0;
	//        final AbstractReplicationStrategy strat = Table.open(table).getReplicationStrategy();
	//        Collection<Range> myRanges = strat.getPendingAddressRanges(tokenMetadata, tokens, address);
	//
	//        Multimap<Range, InetAddress> myRangeAddresses = ArrayListMultimap.create();
	//        Multimap<Range, InetAddress> rangeAddresses = strat.getRangeAddresses(tokenMetadata);
	//        for (Range myRange : myRanges)
	//        {
	//            for (Range range : rangeAddresses.keySet())
	//            {
	//                if (range.contains(myRange))
	//                {
	//                    List<InetAddress> preferred = DatabaseDescriptor.getEndpointSnitch().getSortedListByProximity(address, rangeAddresses.get(range));
	//                    myRangeAddresses.putAll(myRange, preferred);
	//                    break;
	//                }
	//            }
	//            assert myRangeAddresses.keySet().contains(myRange);
	//        }
	//        return myRangeAddresses;
	//    }

	static Collection<Token> getBootstrapTokensFrom(String keyspace, String columnFamily, InetAddress maxEndpoint)
	{
		String body = keyspace + "\t" + columnFamily;
		Message message = new Message(FBUtilities.getBroadcastAddress(),
				StorageService.Verb.BOOTSTRAP_TOKENS, 
				body.getBytes(Charsets.UTF_8),
				Gossiper.instance.getVersion(maxEndpoint));
		int retries = 5;
		long timeout = Math.max(MessagingService.getDefaultCallbackTimeout(), BOOTSTRAP_TIMEOUT);

		while (retries > 0)
		{
			BootstrapTokenCallback btc = new BootstrapTokenCallback();
			MessagingService.instance().sendRR(message, maxEndpoint, btc, timeout);
			Collection<Token> tokens = btc.getTokens(timeout);
			if (tokens!=null && tokens.isEmpty()==false)
				return tokens;

			retries--;
		}
		throw new RuntimeException("Bootstrap failed, could not obtain token from: " + maxEndpoint);
	}

	private static List<InetAddress> getSortedListByTaskNum(
			Set<InetAddress> eps, final Map<InetAddress, Integer> numTasks) 
	{
		List<InetAddress> list = Lists.newArrayList(eps);
		if(list.isEmpty())
			return list;
		
		Collections.sort(list, new Comparator<InetAddress>() 
				{
			@Override
			public int compare(InetAddress o1, InetAddress o2) 
			{
				Integer num1 = numTasks.get(o1);
				Integer num2 = numTasks.get(o2);
				if(num1==null)
					num1 = 0;
				if(num2==null)
					num2 = 0;

				return num1.compareTo(num2);
			}
				});
		
		InetAddress host = list.get(0);
		int num = numTasks.get(host);
		numTasks.put(host, num+1);
		
		return list;
	}

	public static List<InetAddress> getPreferredEndpointsForMigration(Collection<InetAddress> endpoints, Map<InetAddress, Long> volumes)
	{
		return getPreferredEndpointsForMigration(endpoints, volumes, FailureDetector.instance);
	}

	static List<InetAddress> getPreferredEndpointsForMigration(
			Collection<InetAddress> endpoints, 
			Map<InetAddress, Long> volumes, 
			IFailureDetector failureDetector)
			{
		List<Pair<InetAddress, Long>> list = new ArrayList<Pair<InetAddress, Long>>(endpoints.size());
		for(InetAddress ep : endpoints)
		{
			if(!failureDetector.isAlive(ep) || ep.equals(FBUtilities.getBroadcastAddress()))
				continue;

			Long volume = volumes.get(ep);
			if(volume == null)
				volume = 0L;
			Pair<InetAddress, Long> pair = new Pair<InetAddress, Long>(ep, volume);
			list.add(pair);
		}

		Collections.sort(list, new Comparator<Pair<InetAddress, Long>>()
				{
			@Override
			public int compare(Pair<InetAddress, Long> arg0, Pair<InetAddress, Long> arg1) 
			{
				return arg0.right.compareTo(arg1.right);  // the smaller, the better
			}});

		List<InetAddress> results = new ArrayList<InetAddress>(list.size());
		for(Pair<InetAddress, Long> pair : list)
		{
			results.add(pair.left);
		}

		return results;
			}

	public static Multimap<InetAddress, Range> getWorkMap(Multimap<Range, InetAddress> rangesWithSourceTarget)
	{
		return getWorkMap(rangesWithSourceTarget, FailureDetector.instance);
	}

	static Multimap<InetAddress, Range> getWorkMap(Multimap<Range, InetAddress> rangesWithSourceTarget, IFailureDetector failureDetector)
	{
		/*
		 * Map whose key is the source node and the value is a map whose key is the
		 * target and value is the list of ranges to be sent to it.
		 */
		Multimap<InetAddress, Range> sources = ArrayListMultimap.create();

		// TODO look for contiguous ranges and map them to the same source
		for (Range range : rangesWithSourceTarget.keySet())
		{
			for (InetAddress source : rangesWithSourceTarget.get(range))
			{
				// ignore the local IP...
				if (failureDetector.isAlive(source) && !source.equals(FBUtilities.getBroadcastAddress()))
				{
					sources.put(source, range);
					break;
				}
			}
		}
		return sources;
	}

	public static String ringToString(Map<InetAddress, Collection<Token>> ring) 
	{
		TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
		StringBuilder sb = new StringBuilder();

		for(Entry<InetAddress, Collection<Token>> entry : ring.entrySet())
		{
			sb.append(entry.getKey().getHostAddress());
			sb.append(":" + tf.toString(entry.getValue()) + "@");
		}

		return sb.toString();
	}

	public static Map<InetAddress, Collection<Token>> stringToRing(String string) throws UnknownHostException 
	{
		TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
		Map<InetAddress, Collection<Token>> map = new HashMap<InetAddress, Collection<Token>>();

		String[] epStrings = string.split("@", -1);
		for(String epStr : epStrings)
		{
			if(epStr.trim().length()==0)
				continue;

			String[] pieces = epStr.split(":", -1);
			InetAddress endpoint = InetAddress.getByName(pieces[0]);
			Collection<Token> tokens = tf.fromStringToCollection(pieces[1]);
			map.put(endpoint, tokens);
		}

		return map;
	}

	public static class BootstrapTokenVerbHandler implements IVerbHandler
	{
		public void doVerb(Message message, String id)
		{
			String body = new String(message.getMessageBody(), Charsets.UTF_8);
			String[] pieces = body.split("\t", -1);
			Set<Token> tokens = StorageService.instance.getBootstrapTokens(pieces[0], pieces[1]);

			byte[] msgBody = null;
			if(tokens==null || tokens.isEmpty())
			{
				msgBody = ArrayUtils.EMPTY_BYTE_ARRAY;
			}
			else
			{
				String tokensStr = StorageService.getPartitioner().getTokenFactory().toString(tokens);
				msgBody = tokensStr.getBytes(Charsets.UTF_8);
			}

			Message response = message.getInternalReply(msgBody, message.getVersion());
			MessagingService.instance().sendReply(response, id, message.getFrom());
		}
	}

	public static class RingStateVerbHandler implements IVerbHandler
	{
		public void doVerb(Message message, String id)
		{
			String body = new String(message.getMessageBody(), Charsets.UTF_8);
			String[] pieces = body.split("\t", -1);
			Map<InetAddress, Collection<Token>> ring = StorageService.instance.getRingState(pieces[0], pieces[1]);

			byte[] msgBody = null;
			if(ring==null || ring.isEmpty())
			{
				msgBody = ArrayUtils.EMPTY_BYTE_ARRAY;
			}
			else
			{
				String ringStr = ringToString(ring);
				msgBody = ringStr.getBytes(Charsets.UTF_8);
			}

			Message response = message.getInternalReply(msgBody, message.getVersion());
			MessagingService.instance().sendReply(response, id, message.getFrom());
		}
	}

	public static class RemoveTokenVerbHandler implements IVerbHandler
	{
		public void doVerb(Message message, String id)
		{
			String body = new String(message.getMessageBody(), Charsets.UTF_8);
			String[] pieces = body.split("\t", -1);
			String tableName = pieces[0];
			String cfName = pieces[1];
			Token token = StorageService.getPartitioner().getTokenFactory().fromString(pieces[2]);

			StorageService.instance.removeToken(tableName, cfName, token);

			byte[] msgBody = String.valueOf(true).getBytes(Charsets.UTF_8);
			Message response = message.getInternalReply(msgBody, message.getVersion());
			MessagingService.instance().sendReply(response, id, message.getFrom());
		}
	}

	public static class RemovingTokenVerbHandler implements IVerbHandler
	{
		public void doVerb(Message message, String id)
		{
			String body = new String(message.getMessageBody(), Charsets.UTF_8);
			String[] pieces = body.split("\t", -1);
			String tableName = pieces[0];
			String cfName = pieces[1];
			Token token = StorageService.getPartitioner().getTokenFactory().fromString(pieces[2]);

			StorageService.instance.removingToken(tableName, cfName, token);

			byte[] msgBody = String.valueOf(true).getBytes(Charsets.UTF_8);
			Message response = message.getInternalReply(msgBody, message.getVersion());
			MessagingService.instance().sendReply(response, id, message.getFrom());
		}
	}

	public static class SplitRangeVerbHandler implements IVerbHandler
	{
		public void doVerb(Message message, String id)
		{
			String body = new String(message.getMessageBody(), Charsets.UTF_8);
			String[] pieces = body.split("\t", -1);
			assert pieces.length >= 5;

			TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
			Token left = tf.fromString(pieces[2]);
			Token right = tf.fromString(pieces[3]);
			Token token = tf.fromString(pieces[4]);

			Range range = new Range(left, right);

			logger.info("hli@cse:\tProcess SplitRangeVerb for " + pieces[0] + "-" + pieces[1] + 
					": " + range + " using " + token);
			Token split = StorageService.instance.getSplitTokenFor(pieces[0], pieces[1], range, token);

			String tokenStr = tf.toString(split);
			byte[] msgBody = tokenStr.getBytes(Charsets.UTF_8);
			Message response = message.getInternalReply(msgBody, message.getVersion());
			MessagingService.instance().sendReply(response, id, message.getFrom());
		}
	}

	private static class BootstrapTokenCallback implements IAsyncCallback
	{
		private volatile Collection<Token> tokens;
		private final Condition condition = new SimpleCondition();

		public Collection<Token> getTokens(long timeout)
		{
			boolean success;
			try
			{
				success = condition.await(timeout, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e)
			{
				throw new RuntimeException(e);
			}

			return success ? tokens : null;
		}

		public void response(Message msg)
		{
			String str = new String(msg.getMessageBody(), Charsets.UTF_8);
			tokens = StorageService.getPartitioner().getTokenFactory().fromStringToCollection(str);
			condition.signalAll();
		}

		public boolean isLatencyForSnitch()
		{
			return false;
		}
	}

	private static class RingStateCallback implements IAsyncCallback
	{
		private volatile Map<InetAddress, Collection<Token>> ring;
		private final Condition condition = new SimpleCondition();

		public Map<InetAddress, Collection<Token>> getRing(long timeout)
		{
			boolean success;
			try
			{
				success = condition.await(timeout, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e)
			{
				throw new RuntimeException(e);
			}

			return success ? ring : null;
		}

		public void response(Message msg)
		{
			String ringStr = new String(msg.getMessageBody(), Charsets.UTF_8);

			try {
				ring = stringToRing(ringStr);
				condition.signalAll();
			} 
			catch (UnknownHostException e) 
			{
				e.printStackTrace();
			}
		}

		public boolean isLatencyForSnitch()
		{
			return false;
		}
	}

	private static class RemoveTokenCallback implements IAsyncCallback
	{
		private volatile Boolean received;
		private final Condition condition = new SimpleCondition();

		public Boolean getResponse(long timeout)
		{
			boolean success;
			try
			{
				success = condition.await(timeout, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e)
			{
				throw new RuntimeException(e);
			}

			return success ? received : null;
		}

		public void response(Message msg)
		{
			String str = new String(msg.getMessageBody(), Charsets.UTF_8);
			received = Boolean.valueOf(str);
			condition.signalAll();
		}

		public boolean isLatencyForSnitch()
		{
			return false;
		}
	}


	private static class RemovingTokenCallback implements IAsyncCallback
	{
		private volatile Boolean received;
		private final Condition condition = new SimpleCondition();

		public Boolean getResponse(long timeout)
		{
			boolean success;
			try
			{
				success = condition.await(timeout, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e)
			{
				throw new RuntimeException(e);
			}

			return success ? received : null;
		}

		public void response(Message msg)
		{
			String str = new String(msg.getMessageBody(), Charsets.UTF_8);
			received = Boolean.valueOf(str);
			condition.signalAll();
		}

		public boolean isLatencyForSnitch()
		{
			return false;
		}
	}


	private static class SplitRangeCallback implements IAsyncCallback
	{
		private volatile Token token;
		private final Condition condition = new SimpleCondition();

		public Token getToken(long timeout)
		{
			boolean success;
			try
			{
				success = condition.await(timeout, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e)
			{
				throw new RuntimeException(e);
			}

			return success ? token : null;
		}

		public void response(Message msg)
		{
			String str = new String(msg.getMessageBody(), Charsets.UTF_8);
			token = StorageService.getPartitioner().getTokenFactory().fromString(str);
			condition.signalAll();
		}

		public boolean isLatencyForSnitch()
		{
			return false;
		}
	}
}
