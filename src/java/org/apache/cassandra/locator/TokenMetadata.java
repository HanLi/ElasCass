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

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class TokenMetadata
{
	private static Logger logger = LoggerFactory.getLogger(TokenMetadata.class);
	
	/* The TokenMetadata is changed to one per each column family, instead of one per node */
	public final String keyspace;
	public final String columnfamily;

	/* Maintains token to multi-endpoint map of every token in the cluster. */
	private SetMultimap<Token, InetAddress> tokenToEndpointsMap;

	/* Maintains endpoint to multi-token map of every node in the cluster. */
	private SetMultimap<InetAddress, Token> endpointToTokensMap;

	// Prior to CASSANDRA-603, we just had <tt>Map<Range, InetAddress> pendingRanges<tt>,
	// which was added to when a node began bootstrap and removed from when it finished.
	//
	// This is inadequate when multiple changes are allowed simultaneously.  For example,
	// suppose that there is a ring of nodes A, C and E, with replication factor 3.
	// Node D bootstraps between C and E, so its pending ranges will be E-A, A-C and C-D.
	// Now suppose node B bootstraps between A and C at the same time. Its pending ranges
	// would be C-E, E-A and A-B. Now both nodes need to be assigned pending range E-A,
	// which we would be unable to represent with the old Map.  The same thing happens
	// even more obviously for any nodes that boot simultaneously between same two nodes.
	//
	// So, we made two changes:
	//
	// First, we changed pendingRanges to a <tt>Multimap<Range, InetAddress></tt> (now
	// <tt>Map<String, Multimap<Range, InetAddress>></tt>, because replication strategy
	// and options are per-KeySpace).
	//
	// Second, we added the bootstrapTokens and leavingEndpoints collections, so we can
	// rebuild pendingRanges from the complete information of what is going on, when
	// additional changes are made mid-operation.
	//
	// Finally, note that recording the tokens of joining nodes in bootstrapTokens also
	// means we can detect and reject the addition of multiple nodes at the same token
	// before one becomes part of the ring.
//	private SetMultimap<InetAddress, Token> bootstrapEndpoints = Multimaps.synchronizedSetMultimap(
//			HashMultimap.<InetAddress, Token>create());
//	// (don't need to record Token here since it's still part of tokenToEndpointMap until it's done leaving)
//	private Set<InetAddress> leavingEndpoints = new HashSet<InetAddress>();

	// tokens which are joining a node
	private SetMultimap<Token, InetAddress> joiningTokens = 
		Multimaps.synchronizedSetMultimap(HashMultimap.<Token, InetAddress>create());
	// tokens which are leaving a node
	private SetMultimap<Token, InetAddress> leavingTokens = 
		Multimaps.synchronizedSetMultimap(HashMultimap.<Token, InetAddress>create());

	// a pending list of endpoints that should serve write, but are not ready for read
	private SetMultimap<Token, InetAddress> endpointsForWrite;
	
	/* Use this lock for manipulating the token map */
	private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
	private ArrayList<Token> sortedTokens;

	/* list of subscribers that are notified when the tokenToEndpointMap changed */
	private final CopyOnWriteArrayList<AbstractReplicationStrategy> subscribers = new CopyOnWriteArrayList<AbstractReplicationStrategy>();

	public static final Comparator<Token> tokenComparator = 
    	new Comparator<Token>()
    	{
			@Override
			public int compare(Token o1, Token o2) 
			{
				return o1.compareTo(o2);
			}};
	
	public TokenMetadata(String ks, String cf)
	{
		this(ks, cf, null);
	}

	public TokenMetadata(String ks, String cf, SetMultimap<Token, InetAddress> tokenToEndpointsMap)
	{
		this.keyspace = ks;
		this.columnfamily = cf;

		if (tokenToEndpointsMap == null)
			tokenToEndpointsMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Token, InetAddress>create());

		this.tokenToEndpointsMap = tokenToEndpointsMap;
		this.endpointToTokensMap = Multimaps.synchronizedSetMultimap(HashMultimap.<InetAddress, Token>create());
		
		for(Entry<Token, InetAddress> entry : tokenToEndpointsMap.entries())
		{
			this.endpointToTokensMap.put(entry.getValue(), entry.getKey());
		}

		endpointsForWrite = calculateEndpointsForWrite();
		sortedTokens = sortTokens();
	}

	private ArrayList<Token> sortTokens()
	{
		ArrayList<Token> tokens = new ArrayList<Token>(tokenToEndpointsMap.keySet());
		Collections.sort(tokens, tokenComparator);
		return tokens;
	}
	
	private SetMultimap<Token, InetAddress> calculateEndpointsForWrite()
	{
		SetMultimap<Token, InetAddress> writes = Multimaps.synchronizedSetMultimap(
				HashMultimap.<Token, InetAddress>create());
		
		writes.putAll(tokenToEndpointsMap);
		writes.putAll(joiningTokens);
		
		for(Entry<Token, InetAddress> entry : leavingTokens.entries())
		{
			writes.remove(entry.getKey(), entry.getValue());
		}
		
		return writes;
	}
	
	public void replaceRing(Map<InetAddress, Collection<Token>> ring)
	{
		lock.writeLock().lock();
		try
		{
			tokenToEndpointsMap.clear();
			endpointToTokensMap.clear();
			sortedTokens.clear();
//			
//			bootstrapEndpoints.clear();
//			leavingEndpoints.clear();
//			
//			pendingTokens.clear();
			
			joiningTokens.clear();
			leavingTokens.clear();
			
			for(Entry<InetAddress, Collection<Token>> entry : ring.entrySet())
			{
				InetAddress endpoint = entry.getKey();
				for(Token token : entry.getValue())
				{
					tokenToEndpointsMap.put(token, endpoint);
					endpointToTokensMap.put(endpoint, token);
				}
			}
			
			sortedTokens = sortTokens();
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches();
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public void addNormalEndpoint(InetAddress endpoint, Iterable<Token> tokens) 
	{
		assert tokens != null;
		assert endpoint != null;
	
		lock.writeLock().lock();
		try
		{
			Set<Token> oldTokens = endpointToTokensMap.get(endpoint);
			for(Token token : oldTokens)
			{
				tokenToEndpointsMap.remove(token, endpoint);
			}
			endpointToTokensMap.removeAll(endpoint);
			
			for(Token token : tokens)
			{
				joiningTokens.remove(token, endpoint); 
				leavingTokens.remove(token, endpoint);
				
				tokenToEndpointsMap.put(token, endpoint);
				endpointToTokensMap.put(endpoint, token);
			}
//			bootstrapEndpoints.removeAll(endpoint);
//			leavingEndpoints.remove(endpoint);
			
			sortedTokens = sortTokens();
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches();
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	
	public void removeEndpoint(InetAddress endpoint)
		{
			assert endpoint != null;
		
			lock.writeLock().lock();
			try
			{
	//			bootstrapEndpoints.removeAll(endpoint);
				Set<Token> tokens = endpointToTokensMap.get(endpoint);
				for(Token token : tokens) 
				{
					tokenToEndpointsMap.remove(token, endpoint);
					leavingTokens.remove(token, endpoint);
					joiningTokens.remove(token, endpoint);
				}
				endpointToTokensMap.removeAll(endpoint);
	//			leavingEndpoints.remove(endpoint);
		
				sortedTokens = sortTokens();
				endpointsForWrite = calculateEndpointsForWrite();
				invalidateCaches();
			}
			finally
			{
				lock.writeLock().unlock();
			}
		}

	public void addBootstrapEndpoint(InetAddress endpoint, Iterable<Token> tokens)
	{
		assert endpoint != null;
		assert tokens != null;

		lock.writeLock().lock();
		try
		{
//			bootstrapEndpoints.putAll(endpoint, tokens);
//			pendingTokens = calculatePendingTokens();
			for(Token token : tokens)
			{
				joiningTokens.put(token, endpoint);
			}
			sortedTokens = sortTokens();
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches();
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	
//	public void removeBootstrapEndpoint(InetAddress endpoint)
//	{
//		assert endpoint != null;
//
//		lock.writeLock().lock();
//		try
//		{
//			bootstrapEndpoints.removeAll(endpoint);
//			pendingTokens = calculatePendingTokens();
//			invalidateCaches();
//		}
//		finally
//		{
//			lock.writeLock().unlock();
//		}
//	}

	
	public void addLeavingEndpoint(InetAddress endpoint)
	{
		assert endpoint != null;

		lock.writeLock().lock();
		try
		{
			Set<Token> tokens = endpointToTokensMap.get(endpoint);
			for(Token token : tokens)
			{
				leavingTokens.put(token, endpoint);
			}
			if(joiningTokens.containsValue(endpoint))
			{
				List<Entry<Token, InetAddress>> toRemove = Lists.newArrayList();
				for(Entry<Token, InetAddress> entry : joiningTokens.entries())
				{
					if(entry.getValue().equals(endpoint))
						toRemove.add(entry);
				}
				for(Entry<Token, InetAddress> entry : toRemove)
				{
					joiningTokens.remove(entry.getKey(), entry.getValue());
				}
			}
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches();
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	
	public void addLeavingToken(InetAddress endpoint, Token token)
	{
		assert token != null;
		assert endpoint != null;
	
		lock.writeLock().lock();
		try
		{
			joiningTokens.remove(token, endpoint);
			leavingTokens.put(token, endpoint);
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches(token);
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public void removeLeavingToken(InetAddress endpoint, Token token)
	{
		assert token != null;
		assert endpoint != null;
	
		lock.writeLock().lock();
		try
		{
			leavingTokens.remove(token, endpoint);
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches(token);
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public void addNormalToken(InetAddress endpoint, Token token) 
	{
		assert token != null;
		assert endpoint != null;
	
		lock.writeLock().lock();
		try
		{
//			leavingEndpoints.remove(endpoint);
//			bootstrapEndpoints.remove(endpoint, token);
			joiningTokens.remove(token, endpoint); 
			leavingTokens.remove(token, endpoint);
			
			tokenToEndpointsMap.put(token, endpoint);
			endpointToTokensMap.put(endpoint, token);
			
			sortedTokens = sortTokens();
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches(token);
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public void removeNormalToken(InetAddress endpoint, Token token) 
	{
		assert token != null;
		assert endpoint != null;
	
		lock.writeLock().lock();
		try
		{
			joiningTokens.remove(token, endpoint);
			leavingTokens.remove(token, endpoint);
			tokenToEndpointsMap.remove(token, endpoint);
			endpointToTokensMap.remove(endpoint, token);
			
//			if(endpointToTokensMap.containsKey(endpoint)==false)
//			{
//				leavingEndpoints.remove(endpoint);
//			}
			
			sortedTokens = sortTokens();
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches(token);
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public void addJoiningToken(InetAddress endpoint, Token token)
	{
		assert token != null;
		assert endpoint != null;

		lock.writeLock().lock();
		try
		{
			leavingTokens.remove(token, endpoint);
			tokenToEndpointsMap.remove(token, endpoint);
			endpointToTokensMap.remove(endpoint, token);
			
			joiningTokens.put(token, endpoint);
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches(token);
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public void removeJoiningToken(InetAddress endpoint, Token token)
	{
		assert token != null;
		assert endpoint != null;

		lock.writeLock().lock();
		try
		{
			joiningTokens.remove(token, endpoint);
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches(token);
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public boolean isSplittable(Range range)
	{
		List<Token> tokens = getSortedTokens();
		
		int indexOfRight = Collections.binarySearch(tokens, range.right, tokenComparator);
		assert indexOfRight >= 0;
		
		if(indexOfRight == 0)
			indexOfRight = tokens.size();
		
		Token token = tokens.get(indexOfRight - 1);
		
		return !token.equals(range.left);
	}
	
	public Token findAvailableSplitToken(Range range)
	{
		List<Token> tokens = getSortedTokens();
		
		int indexOfRight = Collections.binarySearch(tokens, range.right, tokenComparator);
		if(indexOfRight < 0)
			return null;
		
		if(indexOfRight == 0)
			indexOfRight = tokens.size();
		
		Token token = tokens.get(indexOfRight - 1);
		
		return token.equals(range.left) ? null : token;
	}
	
	public boolean addUniqueSplitToken(InetAddress endpoint, Range range, Token token)
	{
		assert range.contains(token);
		assert !range.right.equals(token);
		
		lock.writeLock().lock();
		try
		{
			if(!tokenToEndpointsMap.containsEntry(range.right, endpoint))
				return false;
			
			if(tokenToEndpointsMap.containsEntry(token, endpoint))
				return false;
			
			int i = Collections.binarySearch(sortedTokens, range.right, tokenComparator);
			assert i >= 0;
			if(i == 0)
				i = sortedTokens.size();
			
			if(!sortedTokens.get(i - 1).equals(range.left))
				return false;
			
			tokenToEndpointsMap.put(token, endpoint);
			endpointToTokensMap.put(endpoint, token);
			
			sortedTokens = sortTokens();
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches(token);
			
			return true;
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}
	
	public Set<Pair<InetAddress,Token>> addSplitToken(InetAddress endpoint, Range range, Token token)
	{
		assert token != null;
		assert endpoint != null;

		Set<Pair<InetAddress, Token>> inserted = new HashSet<Pair<InetAddress, Token>>();
		
		lock.writeLock().lock();
		try
		{
			if(!tokenToEndpointsMap.containsKey(range.right))
			{
				return inserted;
			}
			
			// for the first token inserted, split the range in other nodes as well
			Set<InetAddress> endpoints = tokenToEndpointsMap.get(range.right);
			for(InetAddress ep : endpoints)
			{
				if(!tokenToEndpointsMap.containsEntry(token, ep))
				{
					tokenToEndpointsMap.put(token, ep);
					endpointToTokensMap.put(ep, token);
					inserted.add(new Pair<InetAddress, Token>(ep, token));
				}
			}
			
			sortedTokens = sortTokens();
			endpointsForWrite = calculateEndpointsForWrite();
			invalidateCaches(token);
			
			return inserted;
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}
	
	
	public ArrayList<Token> getSortedTokens()
	{
		lock.readLock().lock();
		try
		{
			return sortedTokens;
		}
		finally
		{
			lock.readLock().unlock();
		}
	}
	
	public Set<Token> getTokens(InetAddress endpoint)
	{
		assert endpoint != null;
		
		lock.readLock().lock();
		try
		{
			return endpointToTokensMap.get(endpoint);
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	public boolean isMember(InetAddress endpoint)
	{
		assert endpoint != null;

		lock.readLock().lock();
		try
		{
			return endpointToTokensMap.containsKey(endpoint);
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

//	public boolean isLeaving(InetAddress endpoint)
//	{
//		assert endpoint != null;
//
//		lock.readLock().lock();
//		try
//		{
//			return leavingEndpoints.contains(endpoint);
//		}
//		finally
//		{
//			lock.readLock().unlock();
//		}
//	}

	public boolean isMoving(InetAddress endpoint)
	{
		assert endpoint != null;

		lock.readLock().lock();
		try
		{
			return joiningTokens.containsValue(endpoint) || leavingTokens.containsValue(endpoint);
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	public boolean isEmpty()
	{
		lock.readLock().lock();
		try
		{
			return tokenToEndpointsMap.isEmpty();
		}
		finally
		{
			lock.readLock().unlock();
		}
	}
	
	/**
	 * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
	 * bootstrap tokens and leaving endpoints are not included in the copy.
	 */
	public TokenMetadata cloneOnlyTokenMap()
	{
		lock.readLock().lock();
		try
		{
			return new TokenMetadata(this.keyspace, this.columnfamily,
					Multimaps.synchronizedSetMultimap(HashMultimap.create(tokenToEndpointsMap)));
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	/**
	 * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
	 * current leave operations have finished.
	 *
	 * @return new token metadata
	 */
	public TokenMetadata cloneAfterAllLeft()
	{
		lock.readLock().lock();
		try
		{
			TokenMetadata allLeftMetadata = cloneOnlyTokenMap();

			for (Entry<Token, InetAddress> entry : leavingTokens.entries())
			{
				allLeftMetadata.removeNormalToken(entry.getValue(), entry.getKey());
			}

			return allLeftMetadata;
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	/**
	 * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
	 * current leave and move operations have finished.
	 *
	 * @return new token metadata
	 */
	public TokenMetadata cloneAfterAllSettled()
	{
		lock.readLock().lock();

		try
		{
			TokenMetadata metadata = cloneOnlyTokenMap();

			for (Entry<Token, InetAddress> entry : joiningTokens.entries())
			{
				metadata.addNormalToken(entry.getValue(), entry.getKey());
			}

			for (Entry<Token, InetAddress> entry : leavingTokens.entries())
			{
				metadata.removeNormalToken(entry.getValue(), entry.getKey());
			}
			
			return metadata;
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	public Set<InetAddress> getEndpoints(Token token)
	{
		lock.readLock().lock();
		try
		{
			return tokenToEndpointsMap.get(token);
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	/**
	 * Find in the ring the first token that is less than the given token
	 * @param token
	 * @return the predecesssor
	 */
	public Token getPredecessor(Token token)
	{
		List<Token> tokens = getSortedTokens();
		assert !tokens.isEmpty();
		
		int index = Collections.binarySearch(tokens, token, tokenComparator);
		if (index < 0)
		{
			index = (index + 1) * (-1);
		}
		if(index == 0)
		{
			index = tokens.size();
		}
		
		return tokens.get(index - 1);
	}

	/**
	 * Find in the ring the first token that is larger than the given token.
	 * @param token
	 * @return the successor
	 */
	public Token getSuccessor(Token token)
	{
		List<Token> tokens = getSortedTokens();
		assert !tokens.isEmpty();
		
		int index = Collections.binarySearch(tokens, token, tokenComparator);
		if (index < 0)
		{
			index = (index + 1) * (-1);
			index = index - 1;
		}
		
		index = (index + 1) % tokens.size();
		return tokens.get(index);
	}

	/**
	 * Find in the ring the first token that is equal to or larger than the given token.
	 * @param token
	 * @return the successor
	 */
	public Token getCanonicalToken(Token token)
	{
		List<Token> tokens = getSortedTokens();
		assert !tokens.isEmpty();
		
		int index = Collections.binarySearch(tokens, token, tokenComparator);
		if (index < 0)
		{
			index = (index + 1) * (-1);
			if (index >= tokens.size())
				index = 0;
		}
		return tokens.get(index);
	}

	public Range getPrimaryRangeFor(Token token)
	{
		List<Token> tokens = getSortedTokens();
		assert !tokens.isEmpty();
		
		int index = Collections.binarySearch(tokens, token, tokenComparator);
		if (index < 0)
		{
			index = (index + 1) * (-1);
			if (index >= tokens.size())
				index = 0;
		}
		
		int pred = index==0 ? tokens.size()-1 : index-1;
		
		return new Range(tokens.get(pred), tokens.get(index));
	}
	
	public Set<Range> getPrimaryRangesFor(InetAddress endpoint)
	{
		Set<Token> tokens = getTokens(endpoint);
		Set<Range> ranges = new HashSet<Range>(tokens.size());
		
		for(Token token : tokens)
		{
			ranges.add(new Range(getPredecessor(token), token));
		}
		return ranges;
	}
	
	public List<Token> getSortedTokensFor(Token from, Token to)
	{
		ArrayList tokens = getSortedTokens();
		assert !tokens.isEmpty();
		
		int indexFrom = firstTokenIndex(tokens, from, false);
		int indexTo = firstTokenIndex(tokens, to, false);
		
		List<Token> tokenList = new ArrayList<Token>();
		
		if(indexFrom <= indexTo)
		{
			for(int i=indexFrom; i<=indexTo; i++)
			{
				tokenList.add((Token) tokens.get(i));
			}
		}
		else
		{
			for(int i=indexFrom; i < tokens.size(); i++)
			{
				tokenList.add((Token) tokens.get(i));
			}
			
			for(int i=0; i<=indexTo; i++)
			{
				tokenList.add((Token) tokens.get(i));
			}
		}
		
		return tokenList;
	}
	
//	public Range getDataViewRangeFor(Token token)
//	{
//		Range range = getPrimaryRangeFor(token);
//		if(isAffectedBySplit(range.right))
//		{
//			range = localPendingSplitRanges.get(range.right);
//		}
//		return range;
//	}
//
//	public List<Range> getSortedDataViewRangesFor(Token from, Token to)
//	{
//		List tokens = getSortedTokens();
//		if(tokens.isEmpty())
//			new RuntimeException("The ring is empty. Cannot find any tokens.");
//		
//		int indexFrom = Collections.binarySearch(tokens, from);
//		int indexTo = Collections.binarySearch(tokens, to);
//
//		if(indexFrom < 0)
//		{
//			indexFrom = - indexFrom - 1;
//		}
//		indexFrom = indexFrom == 0 ? tokens.size() - 1 : indexFrom - 1;
//
//		if(indexTo < 0)
//		{
//			indexTo = (indexTo == - tokens.size() - 1) ? 0 : - indexTo - 1;
//		}
//		
//		List<Range> rangeList = new ArrayList<Range>();
//		Range range;
//		
//		if(indexFrom < indexTo)
//		{
//			for(int i=indexFrom; i<indexTo; i++)
//			{
//				createDataViewRangeFor((Token)tokens.get(i), (Token)tokens.get(i+1), rangeList);
//			}
//		}
//		else if(indexFrom > indexTo)
//		{
//			for(int i=indexFrom; i < tokens.size()-1; i++)
//			{
//				createDataViewRangeFor((Token)tokens.get(i), (Token)tokens.get(i+1), rangeList);
//			}
//			
//			createDataViewRangeFor((Token)tokens.get(tokens.size()-1), (Token)tokens.get(0), rangeList);
//			
//			for(int i=0; i < indexTo; i++)
//			{
//				createDataViewRangeFor((Token)tokens.get(i), (Token)tokens.get(i+1), rangeList);
//			}
//		}
//		else // indexFrom == indexTo
//		{
//			createDataViewRangeFor((Token)tokens.get(indexFrom), (Token)tokens.get(indexTo), rangeList);
//		}
//		
//		return rangeList;
//	}
//
//	private void createDataViewRangeFor(Token left, Token right, List<Range> savedList)
//	{
//		if(!isAffectedBySplit(right))
//		{
//			Range range = new Range(left, right);
//			savedList.add(range);
//		}
//		else
//		{
//			Range range = localPendingSplitRanges.get(right);
//			if(!savedList.contains(range))
//				savedList.add(range);
//		}
//	}
	
//	private Multimap<Token, InetAddress> getPendingTokensMM()
//	{
//		return pendingTokens;
//	}
//
//	/** a mutable map may be returned but caller should not modify it */
//	public Map<Token, Collection<InetAddress>> getPendingTokens()
//	{
//		return getPendingTokensMM().asMap();
//	}
//
//	public Set<Token> getPendingTokens(InetAddress endpoint)
//	{
//		Set<Token> tokens = new HashSet<Token>();
//		for (Entry<Token, InetAddress> entry : getPendingTokensMM().entries())
//		{
//			if (entry.getValue().equals(endpoint))
//			{
//				tokens.add(entry.getKey());
//			}
//		}
//		return tokens;
//	}
//
//	private SetMultimap<InetAddress,Token> getBootstrapEndpointsMM()
//	{
//		return bootstrapEndpoints;
//	}
//	
//	/** caller should not modify this */
//	public Map<InetAddress, Collection<Token>> getBootstrapEndpoints()
//	{
//		return getBootstrapEndpointsMM().asMap();
//	}
	
	private SetMultimap<InetAddress,Token> getNormalEndpointsMM()
	{
		return endpointToTokensMap;
	}
	
	/** caller should not modify this */
	public Map<InetAddress, Collection<Token>> getNormalEndpoints()
	{
		return getNormalEndpointsMM().asMap();
	}

	/** caller should not modify leavingEndpoints */
//	public Set<InetAddress> getLeavingEndpoints()
//	{
//		return leavingEndpoints;
//	}
	
	private SetMultimap<Token,InetAddress> getJoiningTokensMM()
	{
		return joiningTokens;
	}
	
	public Map<Token, Collection<InetAddress>> getJoiningTokens()
	{
		return getJoiningTokensMM().asMap();
	}
	
	private SetMultimap<Token,InetAddress> getLeavingTokensMM()
	{
		return leavingTokens;
	}
	
	public Map<Token, Collection<InetAddress>> getLeavingTokens()
	{
		return getLeavingTokensMM().asMap();
	}

	public static int firstTokenIndex(final ArrayList<Token> ring, Token start, boolean insertMin)
	{
		assert ring.size() > 0;
		// insert the minimum token (at index == -1) if we were asked to include it and it isn't a member of the ring
		int i = Collections.binarySearch(ring, start, tokenComparator);
		if (i < 0)
		{
			i = (i + 1) * (-1);
			if (i >= ring.size())
				i = insertMin ? -1 : 0;
		}
		return i;
	}

	public static Token firstToken(final ArrayList<Token> ring, Token start)
	{
		return ring.get(firstTokenIndex(ring, start, false));
	}

	/**
	 * iterator over the Tokens in the given ring, starting with the token for the node owning start
	 * (which does not have to be a Token in the ring)
	 * @param includeMin True if the minimum token should be returned in the ring even if it has no owner.
	 */
	public static Iterator<Token> ringIterator(final ArrayList<Token> ring, Token start, boolean includeMin)
	{
		if (ring.isEmpty())
		{
			return includeMin ? Iterators.singletonIterator(StorageService.getPartitioner().getMinimumToken())
					: Iterators.<Token>emptyIterator();
		}

		final boolean insertMin = (includeMin && !ring.get(0).equals(StorageService.getPartitioner().getMinimumToken())) ? true : false;
		final int startIndex = firstTokenIndex(ring, start, insertMin);
		return new AbstractIterator<Token>()
		{
			int j = startIndex;
			protected Token computeNext()
			{
				if (j < -1)
					return endOfData();
				try
				{
					// return minimum for index == -1
					if (j == -1)
						return StorageService.getPartitioner().getMinimumToken();
					// return ring token for other indexes
					return ring.get(j);
				}
				finally
				{
					j++;
					if (j == ring.size())
						j = insertMin ? -1 : 0;
					if (j == startIndex)
						// end iteration
						j = -2;
				}
			}
		};
	}

	public void clear()
	{
		lock.writeLock().lock();
		try
		{
			tokenToEndpointsMap.clear();
			endpointToTokensMap.clear();
			sortedTokens.clear();
			endpointsForWrite.clear();
//			bootstrapEndpoints.clear();
//			leavingEndpoints.clear();
			
			joiningTokens.clear();
			leavingTokens.clear();
			
			invalidateCaches();
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		lock.readLock().lock();
		try
		{
			Set<InetAddress> eps = endpointToTokensMap.keySet();

			if (!eps.isEmpty())
			{
				sb.append("Normal Endpoints:");
				sb.append(System.getProperty("line.separator"));
				for (InetAddress ep : eps)
				{
					sb.append(ep + ":");
					sb.append(StringUtils.join(endpointToTokensMap.get(ep), ","));
					sb.append(System.getProperty("line.separator"));
				}
			}
//
//			synchronized (bootstrapEndpoints)
//			{
//				eps = bootstrapEndpoints.keySet();
//				if (!eps.isEmpty())
//				{
//					sb.append("Bootstrapping Endpoints:" );
//					sb.append(System.getProperty("line.separator"));
//
//					for (InetAddress ep : eps)
//					{
//						sb.append(ep + ":");
//						sb.append(StringUtils.join(bootstrapEndpoints.get(ep), ","));
//						sb.append(System.getProperty("line.separator"));
//					}
//				}
//			}
//
//			if (!leavingEndpoints.isEmpty())
//			{
//				sb.append("Leaving Endpoints:");
//				sb.append(System.getProperty("line.separator"));
//				for (InetAddress ep : leavingEndpoints)
//				{
//					sb.append(ep);
//					sb.append(System.getProperty("line.separator"));
//				}
//			}
//
//			if (!pendingTokens.isEmpty())
//			{
//				sb.append("Pending Ranges:");
//				sb.append(System.getProperty("line.separator"));
//				sb.append(printPendingTokens());
//			}
		}
		finally
		{
			lock.readLock().unlock();
		}

		return sb.toString();
	}

//	public String printPendingTokens()
//	{
//		StringBuilder sb = new StringBuilder();
//
//		for (Entry<Token, InetAddress> rmap : pendingTokens.entries())
//		{
//			sb.append(rmap.getValue() + ":" + rmap.getKey());
//			sb.append(System.getProperty("line.separator"));
//		}
//
//		return sb.toString();
//	}

	
	public void invalidateCaches()
	{
		for (AbstractReplicationStrategy subscriber : subscribers)
		{
			subscriber.invalidateCachedTokenEndpointValues();
		}
	}
	
	public void invalidateCaches(Token token)
	{
		for (AbstractReplicationStrategy subscriber : subscribers)
		{
			subscriber.invalidateCachedTokenEndpointValues(token);
		}
	}

	public void register(AbstractReplicationStrategy subscriber)
	{
		subscribers.add(subscriber);
	}

	public void unregister(AbstractReplicationStrategy subscriber)
	{
		subscribers.remove(subscriber);
	}

	/**
	 * write endpoints may be different from read endpoints, because read endpoints only need care about the
	 * "natural" nodes for a token, but write endpoints also need to account for nodes that are bootstrapping
	 * into the ring, and write data there too so that they stay up to date during the bootstrap process.
	 * Thus, this method may return more nodes than the Replication Factor.
	 *
	 * If possible, will return the same collection it was passed, for efficiency.
	 *
	 * Only ReplicationStrategy should care about this method (higher level users should only ask for Hinted).
	 */
	public ArrayList<InetAddress> getEndpointsForWriting(Token canonicalToken)
	{
		lock.readLock().lock();
		try
		{
			return Lists.<InetAddress>newArrayList(endpointsForWrite.get(canonicalToken));
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	/**
	 * @return a list of endpoints to consider for read operations on the cluster.
	 */
	public ArrayList<InetAddress> getEndpointsForReading(Token canonicalToken)
	{
		lock.readLock().lock();
		try
		{
			return Lists.<InetAddress>newArrayList(tokenToEndpointsMap.get(canonicalToken));
		}
		finally
		{
			lock.readLock().unlock();
		}
	}
	
	/**
	 * @return a token to endpoint map to consider for read operations on the cluster.
	 */
	public Map<Token, Collection<InetAddress>> getTokenToEndpointsMapForReading()
	{
		lock.readLock().lock();
		try
		{
			return HashMultimap.<Token, InetAddress>create(tokenToEndpointsMap).asMap();
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

//	/**
//	 * @return a (stable copy, won't be modified) Token to Endpoint map for all the normal and bootstrapping nodes
//	 *         in the cluster.
//	 */
//	public Map<Token, Collection<InetAddress>> getNormalAndBootstrappingTokenToEndpointsMap()
//	{
//		lock.readLock().lock();
//		try
//		{
//			SetMultimap<Token, InetAddress> map = HashMultimap.<Token, InetAddress>create(tokenToEndpointsMap);
//			for(Entry<InetAddress, Token> entry : bootstrapEndpoints.entries())
//			{
//				map.put(entry.getValue(), entry.getKey());
//			}
//			return map.asMap();
//		}
//		finally
//		{
//			lock.readLock().unlock();
//		}
//	}
}
