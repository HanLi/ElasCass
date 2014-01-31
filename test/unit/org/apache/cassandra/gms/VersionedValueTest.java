package org.apache.cassandra.gms;
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


import static org.junit.Assert.*;

import java.util.Set;

import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.VersionedValue.VersionedValueFactory;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import com.google.common.collect.Sets;

public class VersionedValueTest
{
    
    @Test
    public void testVersionedValue()
    {
    	VersionedValueFactory valueFactory = StorageService.instance.valueFactory;
    	BytesToken[] tokenArray = new BytesToken[3];
		tokenArray[0] = new BytesToken(ByteBufferUtil.bytes("00000000"));
		tokenArray[1] = new BytesToken(ByteBufferUtil.bytes("0000ffee"));
		tokenArray[2] = new BytesToken(ByteBufferUtil.bytes("000123de"));
    	
		Range range = new Range(tokenArray[0], tokenArray[2]);
		
    	VersionedValue value = valueFactory.splitRange("testKS", "testCF", range, tokenArray[1], System.currentTimeMillis());
    	System.out.println(value.value);
    	String[] pieces = value.value.split(VersionedValue.DELIMITER_STR, -1);
    	for(int i=0; i<pieces.length; i++)
    		System.out.println(pieces[i]);
    }


}
