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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

public class TokenMetadataTest
{

    @Test
    public void testSplitRange() throws IOException
    {
    	TokenMetadata tm = new TokenMetadata("testKS", "testCF");

    	BytesToken token1 = new BytesToken(ByteBufferUtil.bytes("00000000"));
    	BytesToken token2 = new BytesToken(ByteBufferUtil.bytes("0000ffee"));
    	BytesToken token3 = new BytesToken(ByteBufferUtil.bytes("000123de"));
    	BytesToken token4 = new BytesToken(ByteBufferUtil.bytes("001123de"));
    	BytesToken token5 = new BytesToken(ByteBufferUtil.bytes("003123de"));
    	BytesToken token6 = new BytesToken(ByteBufferUtil.bytes("010123de"));
    	BytesToken token7 = new BytesToken(ByteBufferUtil.bytes("020123de"));
    	
		InetAddress ep1 = InetAddress.getByName("127.0.0.2");
		InetAddress ep2 = InetAddress.getByName("127.0.0.3");
		
		Set<Token> tokens = new HashSet<Token>();
		tokens.add(token1);
		tm.addNormalEndpoint(ep1, tokens);
		tm.addNormalEndpoint(ep2, tokens);
		
		Range range = tm.getPrimaryRangeFor(token1);
		assertFalse(tm.isSplittable(range));
		
		assertTrue(tm.addUniqueSplitToken(ep1, range, token2));
		assertTrue(tm.isSplittable(range));
		
		assertFalse(tm.addUniqueSplitToken(ep2, range, token3));
		assertTrue(tm.isSplittable(range));
		
		Token token = tm.findAvailableSplitToken(range);
		assertEquals(token2, token);
    }
    
}
