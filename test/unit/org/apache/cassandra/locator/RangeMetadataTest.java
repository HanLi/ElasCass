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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.*;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

public class RangeMetadataTest
{
	public static final BytesToken token1 = new BytesToken(ByteBufferUtil.bytes("00000000"));
	public static final BytesToken token2 = new BytesToken(ByteBufferUtil.bytes("0000ffee"));
	public static final BytesToken token3 = new BytesToken(ByteBufferUtil.bytes("000123de"));
	public static final BytesToken token4 = new BytesToken(ByteBufferUtil.bytes("00023456"));
	public static final BytesToken token5 = new BytesToken(ByteBufferUtil.bytes("00123456"));
	public static final BytesToken token6 = new BytesToken(ByteBufferUtil.bytes("00232456"));
	public static final BytesToken token7 = new BytesToken(ByteBufferUtil.bytes("05235677"));
	public static final BytesToken token8 = new BytesToken(ByteBufferUtil.bytes("54688767"));
	
	public static final Range range1 = new Range(token1, token2);
	public static final Range range2 = new Range(token3, token4);
	public static final Range range3 = new Range(token4, token5);
	public static final Range range4 = new Range(token6, token7);
	public static final Range range5 = new Range(token8, token2);
	
	public static final Integer cfId1 = 1001;
	public static final Integer cfId2 = 1002;

    @Test
    public void testAddGet() throws Throwable
    {
    	InetAddress node1 = InetAddress.getByName("127.0.0.2");
    	
		RangeMetadata rm = new RangeMetadata(node1);
		rm.addRange(1001, range1);
		
		Set<Range> ranges = rm.getRanges(cfId1);
		assertEquals(1, ranges.size());
		
		ranges = rm.getRanges(cfId2);
		assertNotNull(ranges);
		assertTrue(ranges.isEmpty());
		
		Set<Range> set = new HashSet<Range>();
		set.add(range1);
		set.add(range2);
		set.add(range3);
		set.add(range4);
		set.add(range5);
		rm.addRanges(cfId1, set);
		
		ranges = rm.getRanges(cfId1);
		assertEquals(5, ranges.size());
    }

    @Test
    public void testRemove() throws Throwable
    {
    	InetAddress node1 = InetAddress.getByName("127.0.0.2");
    	
		RangeMetadata rm = new RangeMetadata(node1);
		rm.addRange(cfId1, range1);
		rm.addRange(cfId1, range2);
		rm.addRange(cfId1, range3);
		
		rm.removeAll(cfId1);
		Set<Range> ranges = rm.getRanges(cfId1);
		assertEquals(0, ranges.size());
		assertTrue(rm.isEmpty());
    }
    
    @Test
    public void testGetAll() throws Throwable
    {
    	InetAddress node1 = InetAddress.getByName("127.0.0.2");
    	
		RangeMetadata rm = new RangeMetadata(node1);
		rm.addRange(cfId1, range1);
		rm.addRange(cfId1, range2);
		rm.addRange(cfId1, range3);
		
		rm.addRange(cfId1, range4);
		rm.addRange(cfId1, range2);
		rm.addRange(cfId1, range3);
		
		Collection<Range> ranges = rm.getAllRanges().get(cfId1);
		assertEquals(4, ranges.size());
    }
}
