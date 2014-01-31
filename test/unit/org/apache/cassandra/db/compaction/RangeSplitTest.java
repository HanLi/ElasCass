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
package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Pair;
import org.junit.Test;

public class RangeSplitTest
{
	public final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	public static final RangeSplitTest instance = new RangeSplitTest();
	
    @Test
    public void testGetWeightedMidToken()
    {
    	List<Pair<Token, Double>> list = new ArrayList<Pair<Token, Double>>();
    	IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
    	
    	for(int i=0; i<10; i++)
    	{
    		Token token = partitioner.getRandomToken();
    		Double weight = Math.random();
    		Pair<Token, Double> pair = new Pair<Token, Double>(token, weight);
    		list.add(pair);
    	}
    	
    	Collections.sort(list, new Comparator<Pair<Token, Double>>()
				{
			@Override
			public int compare(Pair<Token, Double> o1, Pair<Token, Double> o2) 
			{
				// make it descendent
				return o2.right.compareTo(o1.right);
			}});
    	
    	for(Pair<Token, Double> pair : list)
    	{
    		System.out.println(pair.toString());
    	}
    	
		Token midToken = RangeSplitTask.getWeightedMidTokenFor(list);
		
		System.out.println("\n\nThe chosen token is:\t" + midToken);
    }
    
    
    public static void main(String[] args) throws InterruptedException
    {
    	System.out.println("This is the main Thread");
    	
    	Thread th1 = new Thread(new Runnable()
    	{
			@Override
			public void run() 
			{
				RangeSplitTest.instance.lock.readLock().lock();
				
				try {
					for(int i=0; i<10; i++)
					{
						Thread.sleep(200);
						System.out.println("Thread 1:\t" + i);
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				finally {
					RangeSplitTest.instance.lock.readLock().unlock();
				}
			}
    	});
    	th1.start();
    	
    	Thread.sleep(500);
    	
    	Thread th2 = new Thread(new Runnable() {
			@Override
			public void run() 
			{
				RangeSplitTest.instance.lock.writeLock().lock();
				
				try {
					for(int i=0; i<20; i++)
					{
						try {
							Thread.sleep(199);
						} 
						catch (InterruptedException e) 
						{
							e.printStackTrace();
						}
						System.out.println("Thread 2:\t" + i);
						if(i > 6)
							return;
					}
				}
				finally {
					RangeSplitTest.instance.lock.writeLock().unlock();
					System.out.println("Thread 2 can unlock");
				}
			}
    	});
    	th2.start();
    	
    	Thread.sleep(500);
    	
    	Thread th3 = new Thread(new Runnable()
    	{
			@Override
			public void run() 
			{
				RangeSplitTest.instance.lock.readLock().lock();
				
				try {
					for(int i=0; i<10; i++)
					{
						Thread.sleep(179);
						System.out.println("Thread 3:\t" + i);
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				finally {
					RangeSplitTest.instance.lock.readLock().unlock();
				}
			}
    	});
    	th3.start();
    }
}
