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
package org.apache.cassandra.dht;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

public class BootStrapperTest
{
	@Test
	public void testGetRingState() throws IOException
	{
		BytesToken[] tokenArray = new BytesToken[8];
		tokenArray[0] = new BytesToken(ByteBufferUtil.bytes("00000000"));
		tokenArray[1] = new BytesToken(ByteBufferUtil.bytes("0000ffee"));
		tokenArray[2] = new BytesToken(ByteBufferUtil.bytes("000123de"));
		tokenArray[3] = new BytesToken(ByteBufferUtil.bytes("00023456"));
		tokenArray[4] = new BytesToken(ByteBufferUtil.bytes("00123456"));
		tokenArray[5] = new BytesToken(ByteBufferUtil.bytes("00232456"));
		tokenArray[6] = new BytesToken(ByteBufferUtil.bytes("05235677"));
		tokenArray[7] = new BytesToken(ByteBufferUtil.bytes("54688767"));
		
		InetAddress ep1 = InetAddress.getByName("127.0.0.1");
		InetAddress ep2 = InetAddress.getByName("127.0.0.2");
		InetAddress ep3 = InetAddress.getByName("127.0.0.3");
		
		SetMultimap<InetAddress, Token> map = HashMultimap.create();
		map.put(ep1, tokenArray[0]);
		map.put(ep1, tokenArray[1]);
		map.put(ep1, tokenArray[2]);
		map.put(ep2, tokenArray[0]);
		map.put(ep2, tokenArray[4]);
		map.put(ep3, tokenArray[3]);
		map.put(ep3, tokenArray[4]);
		
		String str = BootStrapper.ringToString(map.asMap());
		Map<InetAddress, Collection<Token>> newMap = BootStrapper.stringToRing(str);
		
		assertTrue(newMap.containsKey(ep1));
		assertTrue(newMap.containsKey(ep2));
		assertTrue(newMap.containsKey(ep3));
		
		Collection<Token> tokens = newMap.get(ep1);
		assertEquals(3, tokens.size());
		assertTrue(tokens.contains(tokenArray[0]));
		assertTrue(tokens.contains(tokenArray[1]));
		assertTrue(tokens.contains(tokenArray[2]));
		assertFalse(tokens.contains(tokenArray[3]));
	}
//    @Test
//    public void testTokenRoundtrip() throws Exception
//    {
//        StorageService.instance.initServer(0);
//        // fetch a bootstrap token from the local node
//        assert BootStrapper.getBootstrapTokenFrom(FBUtilities.getBroadcastAddress()) != null;
//    }
//    
//    @Test
//    public void testMulitipleAutomaticBootstraps() throws IOException
//    {
//        StorageService ss = StorageService.instance;
//        generateFakeEndpoints(5);
//        InetAddress[] addrs = new InetAddress[] 
//        {
//            InetAddress.getByName("127.0.0.2"),  
//            InetAddress.getByName("127.0.0.3"),  
//            InetAddress.getByName("127.0.0.4"),  
//            InetAddress.getByName("127.0.0.5"),  
//        };
//        InetAddress[] bootstrapAddrs = new InetAddress[]
//        {
//            InetAddress.getByName("127.0.0.12"),  
//            InetAddress.getByName("127.0.0.13"),  
//            InetAddress.getByName("127.0.0.14"),  
//            InetAddress.getByName("127.0.0.15"),  
//        };
//        Map<InetAddress, Double> load = new HashMap<InetAddress, Double>();
//        for (int i = 0; i < addrs.length; i++)
//            load.put(addrs[i], (double)i+2);
//        
//        // give every node a bootstrap source.
//        for (int i = 3; i >=0; i--)
//        {
//            InetAddress bootstrapSource = BootStrapper.getBootstrapSource(ss.getTokenMetadata(), load);
//            assert bootstrapSource != null;
//            assert bootstrapSource.equals(addrs[i]) : String.format("expected %s but got %s for %d", addrs[i], bootstrapSource, i);
//            assert !ss.getTokenMetadata().getBootstrapTokens().containsValue(bootstrapSource);
//            
//            Range range = ss.getPrimaryRangeForEndpoint(bootstrapSource);
//            Token token = StorageService.getPartitioner().midpoint(range.left, range.right);
//            assert range.contains(token);
//            ss.onChange(bootstrapAddrs[i], ApplicationState.STATUS, StorageService.instance.valueFactory.bootstrapping(token));
//        }
//        
//        // any further attempt to bootsrtap should fail since every node in the cluster is splitting.
//        try
//        {
//            BootStrapper.getBootstrapSource(ss.getTokenMetadata(), load);
//            throw new AssertionError("This bootstrap should have failed.");
//        }
//        catch (RuntimeException ex) 
//        {
//            // success!
//        }
//        
//        // indicate that one of the nodes is done. see if the node it was bootstrapping from is still available.
//        Range range = ss.getPrimaryRangeForEndpoint(addrs[2]);
//        Token token = StorageService.getPartitioner().midpoint(range.left, range.right);
//        ss.onChange(bootstrapAddrs[2], ApplicationState.STATUS, StorageService.instance.valueFactory.normal(token));
//        load.put(bootstrapAddrs[2], 0d);
//        InetAddress addr = BootStrapper.getBootstrapSource(ss.getTokenMetadata(), load);
//        assert addr != null && addr.equals(addrs[2]);
//    }
//
//    @Test
//    public void testGuessToken() throws IOException
//    {
//        StorageService ss = StorageService.instance;
//
//        generateFakeEndpoints(5);
//
//        InetAddress two = InetAddress.getByName("127.0.0.2");
//        InetAddress three = InetAddress.getByName("127.0.0.3");
//        InetAddress four = InetAddress.getByName("127.0.0.4");
//        InetAddress five = InetAddress.getByName("127.0.0.5");
//
//        Map<InetAddress, Double> load = new HashMap<InetAddress, Double>();
//        load.put(two, 2.0);
//        load.put(three, 3.0);
//        load.put(four, 4.0);
//        load.put(five, 5.0);
//
//        TokenMetadata tmd = ss.getTokenMetadata();
//        InetAddress source = BootStrapper.getBootstrapSource(tmd, load);
//        assert five.equals(source) : five + " != " + source;
//
//        InetAddress myEndpoint = InetAddress.getByName("127.0.0.1");
//        Range range5 = ss.getPrimaryRangeForEndpoint(five);
//        Token fakeToken = StorageService.getPartitioner().midpoint(range5.left, range5.right);
//        assert range5.contains(fakeToken);
//        ss.onChange(myEndpoint, ApplicationState.STATUS, StorageService.instance.valueFactory.bootstrapping(fakeToken));
//        tmd = ss.getTokenMetadata();
//
//        InetAddress source4 = BootStrapper.getBootstrapSource(tmd, load);
//        assert four.equals(source4) : four + " != " + source4;
//    }
//
//    @Test
//    public void testSourceTargetComputation() throws UnknownHostException
//    {
//        final int[] clusterSizes = new int[] { 1, 3, 5, 10, 100};
//        for (String table : Schema.instance.getNonSystemTables())
//        {
//            int replicationFactor = Table.open(table).getReplicationStrategy().getReplicationFactor();
//            for (int clusterSize : clusterSizes)
//                if (clusterSize >= replicationFactor)
//                    testSourceTargetComputation(table, clusterSize, replicationFactor);
//        }
//    }
//
//    private void testSourceTargetComputation(String table, int numOldNodes, int replicationFactor) throws UnknownHostException
//    {
//        StorageService ss = StorageService.instance;
//
//        generateFakeEndpoints(numOldNodes);
//        Token myToken = StorageService.getPartitioner().getRandomToken();
//        InetAddress myEndpoint = InetAddress.getByName("127.0.0.1");
//
//        TokenMetadata tmd = ss.getTokenMetadata();
//        assertEquals(numOldNodes, tmd.sortedTokens().size());
//        BootStrapper b = new BootStrapper(myEndpoint, myToken, tmd);
//        Multimap<Range, InetAddress> res = b.getRangesWithSources(table);
//        
//        int transferCount = 0;
//        for (Map.Entry<Range, Collection<InetAddress>> e : res.asMap().entrySet())
//        {
//            assert e.getValue() != null && e.getValue().size() > 0 : StringUtils.join(e.getValue(), ", ");
//            transferCount++;
//        }
//
//        assertEquals(replicationFactor, transferCount);
//        IFailureDetector mockFailureDetector = new IFailureDetector()
//        {
//            public boolean isAlive(InetAddress ep)
//            {
//                return true;
//            }
//
//            public void interpret(InetAddress ep) { throw new UnsupportedOperationException(); }
//            public void report(InetAddress ep) { throw new UnsupportedOperationException(); }
//            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
//            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
//            public void remove(InetAddress ep) { throw new UnsupportedOperationException(); }
//            public void clear(InetAddress ep) { throw new UnsupportedOperationException(); }
//        };
//        Multimap<InetAddress, Range> temp = BootStrapper.getWorkMap(res, mockFailureDetector);
//        // there isn't any point in testing the size of these collections for any specific size.  When a random partitioner
//        // is used, they will vary.
//        assert temp.keySet().size() > 0;
//        assert temp.asMap().values().iterator().next().size() > 0;
//        assert !temp.keySet().iterator().next().equals(myEndpoint);
//    }
//
//    private void generateFakeEndpoints(int numOldNodes) throws UnknownHostException
//    {
//        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
//        tmd.clearUnsafe();
//        IPartitioner<?> p = StorageService.getPartitioner();
//
//        for (int i = 1; i <= numOldNodes; i++)
//        {
//            // leave .1 for myEndpoint
//            tmd.updateNormalToken(p.getRandomToken(), InetAddress.getByName("127.0.0." + (i + 1)));
//        }
//    }
}
