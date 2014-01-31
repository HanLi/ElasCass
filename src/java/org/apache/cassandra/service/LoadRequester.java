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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SimpleCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

public class LoadRequester
{
    static final int BROADCAST_INTERVAL = 60 * 1000;

    public static final LoadRequester instance = new LoadRequester();

    private static final Logger logger_ = LoggerFactory.getLogger(LoadRequester.class);

//    /** 
//     * The Double value in this mapping, is the average read count per second for a given column family ID.
//     * The structure of this mapping is:
//     * column family ID --> endpoint --> readCount
//     */
//    private Map<Integer, Map<InetAddress, Double>> loadInfo_ = new HashMap<Integer, Map<InetAddress, Double>>();

    private LoadRequester() {}
    
    public Map<InetAddress, Double> getSystemCpuUsage(Collection<InetAddress> eps)
    {
    	String body = "CPU";
    	long timeout = MessagingService.getDefaultCallbackTimeout();
    	
    	Map<InetAddress, Double> cpus = Maps.newHashMapWithExpectedSize(eps.size());
    	for(InetAddress endpoint : eps)
    	{
    		Message message = new Message(FBUtilities.getBroadcastAddress(),
                    StorageService.Verb.SYSTEM_LOAD,
                    body.getBytes(Charsets.UTF_8),
                    Gossiper.instance.getVersion(endpoint));
    		
    		SystemLoadCallback callback = new SystemLoadCallback();
            MessagingService.instance().sendRR(message, endpoint, callback, timeout);
            Double usage = callback.getVolume(timeout);
            if (usage != null)
            	cpus.put(endpoint, usage);
            else
            	cpus.put(endpoint, 100.0); // un-responsive means 100% cpu usage
    	}
    	
    	for(Map.Entry<InetAddress, Double> entry : cpus.entrySet())
		{
			logger_.info("System Load (i.e. CPU usage) for " + entry.getKey() 
					+ " is " + entry.getValue() + "%");
		}
    	
    	return cpus;
    }
    
    public Map<Token, Integer> getHotSpotTokens(InetAddress endpoint, String tableName, String cfName)
    {
    	String body = tableName + "\t" + cfName;
    	long timeout = MessagingService.getDefaultCallbackTimeout();
    	
    	Message message = new Message(FBUtilities.getBroadcastAddress(),
                StorageService.Verb.HOTSPOT,
                body.getBytes(Charsets.UTF_8),
                Gossiper.instance.getVersion(endpoint));
		
    	HotSpotCallback callback = new HotSpotCallback();
        MessagingService.instance().sendRR(message, endpoint, callback, timeout);
        Map<Token, Integer> tokens = callback.getTokens(timeout);
        if (tokens == null)
        	tokens = Maps.newHashMap();
        
        return tokens;
    }
    
    public static class SystemLoadVerbHandler implements IVerbHandler
    {
        public void doVerb(Message message, String id)
        {
			double usage = StorageService.instance.getSystemLoad();
			byte[] msgBody = String.valueOf(usage).getBytes(Charsets.UTF_8);
            Message response = message.getInternalReply(msgBody, message.getVersion());
            MessagingService.instance().sendReply(response, id, message.getFrom());
        }
    }
    
    
    private static class SystemLoadCallback implements IAsyncCallback
	{
	    private volatile Double cpuUsage;
	    private final Condition condition = new SimpleCondition();
	
	    public Double getVolume(long timeout)
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
	
	        return success ? cpuUsage : null;
	    }
	
	    public void response(Message msg)
	    {
	    	String usage = new String(msg.getMessageBody(), Charsets.UTF_8);
	    	cpuUsage = Double.valueOf(usage);
			condition.signalAll();
	    }
	
	    public boolean isLatencyForSnitch()
	    {
	        return false;
	    }
	}


	public static class HotSpotVerbHandler implements IVerbHandler
    {
        public void doVerb(Message message, String id)
        {
        	String body = new String(message.getMessageBody(), Charsets.UTF_8);
			String[] pieces = body.split("\t", -1);
			Map<Token, Integer> tokens = StorageService.instance.getHotSpotTokens(pieces[0], pieces[1]);
        	
			TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
			StringBuffer msgStr = new StringBuffer();
			
			for(Map.Entry<Token, Integer> entry : tokens.entrySet())
			{
				msgStr.append(tf.toString(entry.getKey()) + "=" + entry.getValue());
				msgStr.append(";");
			}
			
			byte[] msgBody = msgStr.toString().getBytes(Charsets.UTF_8);
            Message response = message.getInternalReply(msgBody, message.getVersion());
            MessagingService.instance().sendReply(response, id, message.getFrom());
        }
    }
	
	private static class HotSpotCallback implements IAsyncCallback
	{
	    private volatile Map<Token, Integer> hotTokens;
	    private final Condition condition = new SimpleCondition();
	
	    public Map<Token, Integer> getTokens(long timeout)
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
	
	        return success ? hotTokens : null;
	    }
	
	    public void response(Message msg)
	    {
	    	String msgStr = new String(msg.getMessageBody(), Charsets.UTF_8);
	    	String[] tokenStr = msgStr.split(";");
	    	
	    	hotTokens = new HashMap<Token, Integer>();
	    	TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
	    	for(int i=0; i < tokenStr.length; i++)
	    	{
	    		if(tokenStr[i].trim().equals("") || !tokenStr[i].contains("="))
	    			continue;
	    		
	    		String[] pair = tokenStr[i].trim().split("=");
	    		Token token = tf.fromString(pair[0]);
	    		int load = Integer.parseInt(pair[1]);
	    		hotTokens.put(token, load);
	    	}
	    	
			condition.signalAll();
	    }
	
	    public boolean isLatencyForSnitch()
	    {
	        return false;
	    }
	}
}

