/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.locator;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.DatacenterSyncWriteResponseHandler;
import org.apache.cassandra.service.DatacenterWriteResponseHandler;
import org.apache.cassandra.service.IWriteResponseHandler;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * A abstract parent for all replication strategies.
*/
public abstract class AbstractReplicationStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReplicationStrategy.class);

    public final String table;
    public final String columnFamily;
    public final Map<String, String> configOptions;
    private final TokenMetadata tokenMetadata;

    public IEndpointSnitch snitch;

    AbstractReplicationStrategy(String table, String cfName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        assert table != null;
        assert snitch != null;
        assert tokenMetadata != null;
        
        this.tokenMetadata = tokenMetadata;
        this.snitch = snitch;
        this.tokenMetadata.register(this);
        this.configOptions = configOptions;
        this.table = table;
        this.columnFamily = cfName;
    }

    private final Map<Token, ArrayList<InetAddress>> cachedReadEndpoints 
    					= new NonBlockingHashMap<Token, ArrayList<InetAddress>>();
    private final Map<Token, ArrayList<InetAddress>> cachedWriteEndpoints 
    					= new NonBlockingHashMap<Token, ArrayList<InetAddress>>();
    
    public ArrayList<InetAddress> getCachedReadEndpoints(Token t)
    {
        return cachedReadEndpoints.get(t);
    }
    
    public ArrayList<InetAddress> getCachedWriteEndpoints(Token t)
    {
        return cachedWriteEndpoints.get(t);
    }

    public void cacheReadEndpoint(Token t, ArrayList<InetAddress> addr)
    {
        cachedReadEndpoints.put(t, addr);
    }
    
    public void cacheWriteEndpoint(Token t, ArrayList<InetAddress> addr)
    {
        cachedWriteEndpoints.put(t, addr);
    }

    public void clearReadEndpointCache()
    {
        logger.debug("clearing cached read endpoints");
        cachedReadEndpoints.clear();
    }
    
    public void clearWriteEndpointCache()
    {
        logger.debug("clearing cached write endpoints");
        cachedWriteEndpoints.clear();
    }

    /**
     * get the (possibly cached) endpoints that should store the given Token
     * Note that while the endpoints are conceptually a Set (no duplicates will be included),
     * we return a List to avoid an extra allocation when sorting by proximity later
     * @param searchToken the token the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public ArrayList<InetAddress> getNaturalEndpoints(Token searchToken)
    {
    	Token canonicalToken = tokenMetadata.getCanonicalToken(searchToken);
    	ArrayList<InetAddress> endpoints = getCachedReadEndpoints(canonicalToken);
        if (endpoints == null)
        {
            TokenMetadata tokenMetadataClone = tokenMetadata.cloneOnlyTokenMap();
            canonicalToken = tokenMetadataClone.getCanonicalToken(searchToken);
            endpoints = new ArrayList<InetAddress>(calculateNaturalEndpoints(canonicalToken, tokenMetadataClone));
            cacheReadEndpoint(canonicalToken, endpoints);
        }

        return new ArrayList<InetAddress>(endpoints);
    }

    public ArrayList<InetAddress> getWriteEndpoints(ByteBuffer key)
    {
        return getWriteEndpoints(StorageService.getPartitioner().getToken(key));
    }
    
    private ArrayList<InetAddress> getWriteEndpoints(Token searchToken)
    {
    	Token canonicalToken = tokenMetadata.getCanonicalToken(searchToken);
    	ArrayList<InetAddress> endpoints = getCachedWriteEndpoints(canonicalToken);
        if (endpoints == null)
        {
//        	ArrayList<InetAddress> naturalEndpoints = getNaturalEndpoints(searchToken);
//        	endpoints = tokenMetadata.getWriteEndpoints(canonicalToken, naturalEndpoints);
        	endpoints = tokenMetadata.getEndpointsForWriting(canonicalToken);
            cacheWriteEndpoint(canonicalToken, endpoints);
        }

        return new ArrayList<InetAddress>(endpoints);
    }
    
    /**
     * calculate the natural endpoints for the given token
     *
     * @see #getNaturalEndpoints(org.apache.cassandra.dht.Token)
     *
     * @param searchToken the token the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public abstract List<InetAddress> calculateNaturalEndpoints(Token<?> searchToken, TokenMetadata tokenMetadata);

    public IWriteResponseHandler getWriteResponseHandler(Collection<InetAddress> writeEndpoints, ConsistencyLevel consistency_level)
    {
        if (consistency_level == ConsistencyLevel.LOCAL_QUORUM)
        {
            // block for in this context will be localnodes block.
            return DatacenterWriteResponseHandler.create(writeEndpoints, consistency_level, table, columnFamily);
        }
        else if (consistency_level == ConsistencyLevel.EACH_QUORUM)
        {
            return DatacenterSyncWriteResponseHandler.create(writeEndpoints, consistency_level, table, columnFamily);
        }
        return WriteResponseHandler.create(writeEndpoints, consistency_level, table, columnFamily);
    }

    /**
     * calculate the RF based on strategy_options. When overwriting, ensure that this get()
     *  is FAST, as this is called often.
     *
     * @return the replication factor
     */
    public abstract int getReplicationFactor();

    /*
     * NOTE: this is pretty inefficient. also the inverse (getRangeAddresses) below.
     * this is fine as long as we don't use this on any critical path.
     * (fixing this would probably require merging tokenmetadata into replicationstrategy,
     * so we could cache/invalidate cleanly.)
     */
    public Multimap<InetAddress, Range> getAddressRanges(TokenMetadata metadata)
    {
        Multimap<InetAddress, Range> map = HashMultimap.create();

        for (Token token : metadata.getSortedTokens())
        {
            Range range = metadata.getPrimaryRangeFor(token);
            List<InetAddress> endpoints = calculateNaturalEndpoints(token, metadata);
            for (InetAddress ep : endpoints)
            {
                map.put(ep, range);
            }
        }

        return map;
    }

    public Multimap<Range, InetAddress> getRangeAddresses(TokenMetadata metadata)
    {
        Multimap<Range, InetAddress> map = HashMultimap.create();

        for (Token token : metadata.getSortedTokens())
        {
            Range range = metadata.getPrimaryRangeFor(token);
            List<InetAddress> endpoints = calculateNaturalEndpoints(token, metadata);
            map.putAll(range, endpoints);
        }

        return map;
    }

    public Multimap<InetAddress, Range> getAddressRanges()
    {
        return getAddressRanges(tokenMetadata);
    }

    public Collection<Range> getPendingAddressRanges(TokenMetadata metadata, Token pendingToken, InetAddress endpoint)
    {
        TokenMetadata temp = metadata.cloneOnlyTokenMap();
        temp.addNormalToken(endpoint, pendingToken);
        return getAddressRanges(temp).get(endpoint);
    }

    public void invalidateCachedTokenEndpointValues()
    {
        clearReadEndpointCache();
        clearWriteEndpointCache();
    }
    
    public void invalidateCachedTokenEndpointValues(Token token)
    {
    	cachedReadEndpoints.remove(token);
    	cachedWriteEndpoints.remove(token);
    }

    public abstract void validateOptions() throws ConfigurationException;

    public static AbstractReplicationStrategy createReplicationStrategy(String table,
    																	String cfName,
                                                                        Class<? extends AbstractReplicationStrategy> strategyClass,
                                                                        TokenMetadata tokenMetadata,
                                                                        IEndpointSnitch snitch,
                                                                        Map<String, String> strategyOptions)
            throws ConfigurationException
    {
        AbstractReplicationStrategy strategy;
        Class [] parameterTypes = new Class[] {String.class, String.class, TokenMetadata.class, IEndpointSnitch.class, Map.class};
        try
        {
            Constructor<? extends AbstractReplicationStrategy> constructor = strategyClass.getConstructor(parameterTypes);
            strategy = constructor.newInstance(table, cfName, tokenMetadata, snitch, strategyOptions);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        // Throws Config Exception if strat_opts don't contain required info
        strategy.validateOptions();

        return strategy;
    }

    public static AbstractReplicationStrategy createReplicationStrategy(String table,
    																	String cfName,
                                                                        String strategyClassName,
                                                                        TokenMetadata tokenMetadata,
                                                                        IEndpointSnitch snitch,
                                                                        Map<String, String> strategyOptions)
            throws ConfigurationException
    {
        Class<AbstractReplicationStrategy> c = getClass(strategyClassName);
        return createReplicationStrategy(table, cfName, c, tokenMetadata, snitch, strategyOptions);
    }

    public static Class<AbstractReplicationStrategy> getClass(String cls) throws ConfigurationException
    {
        String className = cls.contains(".") ? cls : "org.apache.cassandra.locator." + cls;
        return FBUtilities.classForName(className, "replication strategy");
    }

    protected void validateReplicationFactor(String rf) throws ConfigurationException
    {
        try
        {
            if (Integer.parseInt(rf) < 0)
            {
                throw new ConfigurationException("Replication factor must be non-negative; found " + rf);
            }
        }
        catch (NumberFormatException e2)
        {
            throw new ConfigurationException("Replication factor must be numeric; found " + rf);
        }
    }
}
