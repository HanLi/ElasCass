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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SimpleCondition;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

public class DataVolumeRequester
{
    static final int BROADCAST_INTERVAL = 60 * 1000;

    public static final DataVolumeRequester instance = new DataVolumeRequester();

//    /** 
//     * The long-integer value in this mapping, is the disk space used for a given column family ID.
//     * The structure of this mapping is:
//     * column family ID --> endpoint --> spaceUsed
//     */
//    private Map<Integer, Map<InetAddress, Long>> volumeInfo_ = new HashMap<Integer, Map<InetAddress, Long>>();

    private DataVolumeRequester() {}
    
    public Map<InetAddress, Long> getVolumeInfo(String keyspace, String columnFamily, Collection<InetAddress> eps)
    {
    	String body = keyspace + "\t" + columnFamily;
    	long timeout = MessagingService.getDefaultCallbackTimeout();
    	
    	Map<InetAddress, Long> volumes = Maps.newHashMapWithExpectedSize(eps.size());
    	for(InetAddress endpoint : eps)
    	{
    		Message message = new Message(FBUtilities.getBroadcastAddress(),
                    StorageService.Verb.VOLUME,
                    body.getBytes(Charsets.UTF_8),
                    Gossiper.instance.getVersion(endpoint));
    		
    		DataVolumeCallback dvc = new DataVolumeCallback();
            MessagingService.instance().sendRR(message, endpoint, dvc, timeout);
            Long volume = dvc.getVolume(timeout);
            if (volume != null)
            	volumes.put(endpoint, volume);
            else
            	volumes.put(endpoint, 0L);
    	}
    	return volumes;
    }
    
    
    public static class DataVolumeVerbHandler implements IVerbHandler
    {
        public void doVerb(Message message, String id)
        {
        	String body = new String(message.getMessageBody(), Charsets.UTF_8);
			String[] pieces = body.split("\t", -1);
			long volume = StorageService.instance.getDataVolume(pieces[0], pieces[1]);
			
			byte[] msgBody = String.valueOf(volume).getBytes(Charsets.UTF_8);
            Message response = message.getInternalReply(msgBody, message.getVersion());
            MessagingService.instance().sendReply(response, id, message.getFrom());
        }
    }
    
    
    private static class DataVolumeCallback implements IAsyncCallback
    {
        private volatile Long volume;
        private final Condition condition = new SimpleCondition();

        public Long getVolume(long timeout)
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

            return success ? volume : null;
        }

        public void response(Message msg)
        {
        	String volumeStr = new String(msg.getMessageBody(), Charsets.UTF_8);
        	volume = Long.valueOf(volumeStr);
			condition.signalAll();
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }
    }
}

