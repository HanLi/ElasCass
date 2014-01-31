package org.apache.cassandra.db;
/*
 * 
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
 * 
 */


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;

public class SystemTableTest
{
	@Test
    public void testColumnFamily() throws ConfigurationException, IOException
    {
		DatabaseDescriptor.getSeeds();
		SystemTable.checkHealth();
		
		BytesToken[] tokenArray = new BytesToken[8];
		tokenArray[0] = new BytesToken(ByteBufferUtil.bytes("00000000"));
		tokenArray[1] = new BytesToken(ByteBufferUtil.bytes("0000ffee"));
		tokenArray[2] = new BytesToken(ByteBufferUtil.bytes("000123de"));
		tokenArray[3] = new BytesToken(ByteBufferUtil.bytes("00023456"));
		tokenArray[4] = new BytesToken(ByteBufferUtil.bytes("00123456"));
		tokenArray[5] = new BytesToken(ByteBufferUtil.bytes("00232456"));
		tokenArray[6] = new BytesToken(ByteBufferUtil.bytes("05235677"));
		tokenArray[7] = new BytesToken(ByteBufferUtil.bytes("54688767"));
		
		Set<Token> tokens = new HashSet<Token>();
		for(int i=0; i<6; i++)
		{
			tokens.add(tokenArray[i]);
		}
		
		SystemTable.addColumnFamily("testKS3", "testCF1", tokens);
		SystemTable.addTokenToLocalEndpoint("testKS3", "testCF1", tokenArray[7]);
		Collection<Token> results = SystemTable.getTokensForColumnFamily("testKS3", "testCF1");
		
        assertEquals(7, results.size());
        assertTrue(results.containsAll(tokens));
        assertTrue(results.contains(tokenArray[7]));
    }
//
//	@Test
//    public void testNonLocalColumnFamily() throws ConfigurationException, IOException
//    {
//		DatabaseDescriptor.getSeeds();
//        SystemTable.addNonLocalColumnFamily("testKS1", "testCF1");
//        SystemTable.addNonLocalColumnFamily("testKS1", "testCF2");
//        SystemTable.addNonLocalColumnFamily("testKS2", "testCF4");
//        SystemTable.addNonLocalColumnFamily("testKS2", "testCF5");
//        SystemTable.addNonLocalColumnFamily("testKS2", "testCF6");
//        
//        Map<String, Collection<String>> map = SystemTable.getNonLocalColumnFamilies();
//        assertEquals(2, map.size());
//        assertTrue(map.containsKey("testKS1"));
//        assertTrue(map.containsKey("testKS2"));
//        
//        Collection<String> ks1 = map.get("testKS1");
//        assertEquals(2, ks1.size());
//        assertTrue(ks1.contains("testCF1"));
//        assertTrue(ks1.contains("testCF2"));
//        
//        Collection<String> ks2 = map.get("testKS2");
//        assertEquals(3, ks2.size());
//        assertTrue(ks2.contains("testCF4"));
//        assertTrue(ks2.contains("testCF5"));
//        assertTrue(ks2.contains("testCF6"));
//        
//        SystemTable.removeNonLocalColumnFamily("testKS2", "testCF6");
//        SystemTable.removeNonLocalColumnFamily("testKS2", "testCF5");
//        ks2 = SystemTable.getNonLocalColumnFamilies().get("testKS2");
//        assertEquals(ks2.size(), 1);
//        
//        SystemTable.removeNonLocalColumnFamily("testKS1", "testCF1");
//        SystemTable.removeNonLocalColumnFamily("testKS1", "testCF2");
//        ks1 = SystemTable.getNonLocalColumnFamilies().get("testKS1");
//        assertNull(ks1);
//    }
//	
//	@Test
//    public void testLocalRange()
//    {
//		DatabaseDescriptor.getSeeds();
//		BytesToken token1 = new BytesToken(ByteBufferUtil.bytes("00000000"));
//		BytesToken token2 = new BytesToken(ByteBufferUtil.bytes("0000ffee"));
//		BytesToken token3 = new BytesToken(ByteBufferUtil.bytes("000123de"));
//		BytesToken token4 = new BytesToken(ByteBufferUtil.bytes("00023456"));
//		BytesToken token5 = new BytesToken(ByteBufferUtil.bytes("00123456"));
//		BytesToken token6 = new BytesToken(ByteBufferUtil.bytes("00232456"));
//		BytesToken token7 = new BytesToken(ByteBufferUtil.bytes("05235677"));
//		BytesToken token8 = new BytesToken(ByteBufferUtil.bytes("54688767"));
//		
//		Range range1 = new Range(token1, token2);
//		Range range2 = new Range(token3, token4);
//		Range range3 = new Range(token4, token5);
//		Range range4 = new Range(token6, token7);
//		Range range5 = new Range(token8, token2);
//			
//        SystemTable.addLocalRange("testKS1", "testCF1", range1);
//        SystemTable.addLocalRange("testKS1", "testCF1", range2);
//        SystemTable.addLocalRange("testKS2", "testCF1", range3);
//        SystemTable.addLocalRange("testKS2", "testCF1", range4);
//        SystemTable.addLocalRange("testKS2", "testCF1", range5);
//        
//        Collection<Range> ranges = SystemTable.getLocalRanges("testKS2", "testCF1");
//        assertEquals(3, ranges.size());
//        assertTrue(ranges.contains(range3));
//        assertTrue(ranges.contains(range4));
//        assertTrue(ranges.contains(range5));
//        assertFalse(ranges.contains(range2));
//        
//        SystemTable.splitLocalRange("testKS2", "testCF1", range5, token1);
//        ranges = SystemTable.getLocalRanges("testKS2", "testCF1");
//        assertEquals(4, ranges.size());
//        assertTrue(ranges.contains(new Range(token8, token1)));
//        assertTrue(ranges.contains(range1));
//        assertFalse(ranges.contains(range5));
//        
//        SystemTable.removeLocalRange("testKS1", "testCF1", range1);
//        SystemTable.removeLocalRange("testKS1", "testCF1", range2);
//        ranges = SystemTable.getLocalRanges("testKS1", "testCF1");
//        assertEquals(0, ranges.size());
//        assertFalse(ranges.contains(range1));
//    }
//	
//	
//	@Test
//    public void testNonLocalRange() throws UnknownHostException
//    {
//		DatabaseDescriptor.getSeeds();
//		BytesToken token1 = new BytesToken(ByteBufferUtil.bytes("00000000"));
//		BytesToken token2 = new BytesToken(ByteBufferUtil.bytes("0000ffee"));
//		BytesToken token3 = new BytesToken(ByteBufferUtil.bytes("000123de"));
//		BytesToken token4 = new BytesToken(ByteBufferUtil.bytes("00023456"));
//		BytesToken token5 = new BytesToken(ByteBufferUtil.bytes("00123456"));
//		BytesToken token6 = new BytesToken(ByteBufferUtil.bytes("00232456"));
//		BytesToken token7 = new BytesToken(ByteBufferUtil.bytes("05235677"));
//		BytesToken token8 = new BytesToken(ByteBufferUtil.bytes("54688767"));
//		
//		Range range1 = new Range(token1, token2);
//		Range range2 = new Range(token3, token4);
//		Range range3 = new Range(token4, token5);
//		Range range4 = new Range(token6, token7);
//		Range range5 = new Range(token8, token2);
//		
//		InetAddress node1 = InetAddress.getByName("127.0.0.2");
//		InetAddress node2 = InetAddress.getByName("127.0.0.3");
//		
//        SystemTable.addNonLocalRange("testKS1", "testCF1", node1, range1);
//        SystemTable.addNonLocalRange("testKS1", "testCF1", node1, range2);
//        SystemTable.addNonLocalRange("testKS2", "testCF1", node1, range3);
//        SystemTable.addNonLocalRange("testKS2", "testCF1", node1, range4);
//        SystemTable.addNonLocalRange("testKS2", "testCF1", node2, range5);
//        
//        Map<InetAddress, Collection<Range>> nodes = SystemTable.getNonLocalRanges("testKS2", "testCF1");
//        assertEquals(2, nodes.size());
//        Collection<Range> ranges = nodes.get(node1);
//        assertEquals(2, ranges.size());
//        assertTrue(ranges.contains(range3));
//        assertTrue(ranges.contains(range4));
//        assertFalse(ranges.contains(range5));
//        
//        SystemTable.splitNonLocalRange("testKS2", "testCF1", node2, range5, token1);
//        nodes = SystemTable.getNonLocalRanges("testKS2", "testCF1");
//        ranges = nodes.get(node2);
//        assertEquals(2, ranges.size());
//        assertTrue(ranges.contains(new Range(token8, token1)));
//        assertTrue(ranges.contains(range1));
//        assertFalse(ranges.contains(range5));
//        
//        SystemTable.removeNonLocalRange("testKS1", "testCF1", node1, range1);
//        ranges = SystemTable.getNonLocalRanges("testKS1", "testCF1").get(node1);
//        assertEquals(1, ranges.size());
//        assertFalse(ranges.contains(range1));
//    }
	@Test
	public void testArrayListMultimap()
	{
		ArrayListMultimap<Integer, String> map = ArrayListMultimap.<Integer, String>create();
		map.put(1, "String1");
		map.put(1, "String1");
		map.put(1, "String2");
		map.put(2, "String2");
		
		assertEquals(4, map.size());
		assertNotNull(map.get(3));
		assertTrue(map.get(4).isEmpty());
	}
}
