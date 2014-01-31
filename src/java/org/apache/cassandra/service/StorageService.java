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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CounterMutationVerbHandler;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DefinitionsUpdateVerbHandler;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.ReadRepairVerbHandler;
import org.apache.cassandra.db.ReadVerbHandler;
import org.apache.cassandra.db.RowMutationVerbHandler;
import org.apache.cassandra.db.SchemaCheckVerbHandler;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.TruncateVerbHandler;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.GossipDigestAck2VerbHandler;
import org.apache.cassandra.gms.GossipDigestAckVerbHandler;
import org.apache.cassandra.gms.GossipDigestSynVerbHandler;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.gms.VersionedValue.VersionedValueFactory;
import org.apache.cassandra.io.sstable.SSTableDeletingTask;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ResponseVerbHandler;
import org.apache.cassandra.service.AntiEntropyService.TreeRequestVerbHandler;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.streaming.ReplicationFinishedVerbHandler;
import org.apache.cassandra.streaming.StreamOut;
import org.apache.cassandra.streaming.StreamReplyVerbHandler;
import org.apache.cassandra.streaming.StreamRequestVerbHandler;
import org.apache.cassandra.streaming.StreamingRepairTask;
import org.apache.cassandra.streaming.StreamingService;
import org.apache.cassandra.thrift.Constants;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NodeId;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

/*
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public class StorageService implements IEndpointStateChangeSubscriber, StorageServiceMBean
{
    private static Logger logger_ = LoggerFactory.getLogger(StorageService.class);

    public static final int RING_DELAY = getRingDelay(); // delay after which we assume ring has stablized

    /* All verb handler identifiers */
    public enum Verb
    {
        MUTATION,
        BINARY, // Deprecated
        READ_REPAIR,
        READ,
        REQUEST_RESPONSE, // client-initiated reads and writes
        STREAM_INITIATE, // Deprecated
        STREAM_INITIATE_DONE, // Deprecated
        STREAM_REPLY,
        STREAM_REQUEST,
        RANGE_SLICE,
        BOOTSTRAP_TOKENS,
        TREE_REQUEST,
        TREE_RESPONSE,
        JOIN, // Deprecated
        GOSSIP_DIGEST_SYN,
        GOSSIP_DIGEST_ACK,
        GOSSIP_DIGEST_ACK2,
        DEFINITIONS_ANNOUNCE, // Deprecated
        DEFINITIONS_UPDATE,
        TRUNCATE,
        SCHEMA_CHECK,
        INDEX_SCAN,
        REPLICATION_FINISHED,
        INTERNAL_RESPONSE, // responses to internal calls
        COUNTER_MUTATION,
        STREAMING_REPAIR_REQUEST,
        STREAMING_REPAIR_RESPONSE,
        // use as padding for backwards compatability where a previous version needs to validate a verb from the future.
        RING,
        VOLUME,
        REMOVE_TOKEN,
        REMOVING_TOKEN,
        SPLIT,
        SYSTEM_LOAD,
        HOTSPOT,
        ;
        // remember to add new verbs at the end, since we serialize by ordinal
    }
    public static final Verb[] VERBS = Verb.values();

    public static final EnumMap<StorageService.Verb, Stage> verbStages = new EnumMap<StorageService.Verb, Stage>(StorageService.Verb.class)
    {{
        put(Verb.MUTATION, Stage.MUTATION);
        put(Verb.BINARY, Stage.MUTATION);
        put(Verb.READ_REPAIR, Stage.MUTATION);
        put(Verb.READ, Stage.READ);
        put(Verb.REQUEST_RESPONSE, Stage.REQUEST_RESPONSE);
        put(Verb.STREAM_REPLY, Stage.MISC); // TODO does this really belong on misc? I've just copied old behavior here
        put(Verb.STREAM_REQUEST, Stage.STREAM);
        put(Verb.RANGE_SLICE, Stage.READ);
        put(Verb.BOOTSTRAP_TOKENS, Stage.MISC);
        put(Verb.TREE_REQUEST, Stage.ANTI_ENTROPY);
        put(Verb.TREE_RESPONSE, Stage.ANTI_ENTROPY);
        put(Verb.STREAMING_REPAIR_REQUEST, Stage.ANTI_ENTROPY);
        put(Verb.STREAMING_REPAIR_RESPONSE, Stage.ANTI_ENTROPY);
        put(Verb.GOSSIP_DIGEST_ACK, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_ACK2, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_SYN, Stage.GOSSIP);
        put(Verb.DEFINITIONS_UPDATE, Stage.READ);
        put(Verb.TRUNCATE, Stage.MUTATION);
        put(Verb.SCHEMA_CHECK, Stage.MIGRATION);
        put(Verb.INDEX_SCAN, Stage.READ);
        put(Verb.REPLICATION_FINISHED, Stage.MISC);
        put(Verb.INTERNAL_RESPONSE, Stage.INTERNAL_RESPONSE);
        put(Verb.COUNTER_MUTATION, Stage.MUTATION);
        put(Verb.RING, Stage.MISC);
        put(Verb.VOLUME, Stage.MISC);
        put(Verb.SYSTEM_LOAD, Stage.MISC);
        put(Verb.HOTSPOT, Stage.MISC);
        put(Verb.REMOVE_TOKEN, Stage.MISC);
        put(Verb.REMOVING_TOKEN, Stage.MISC);
        put(Verb.SPLIT, Stage.MISC);
    }};

    private static int getRingDelay()
    {
        String newdelay = System.getProperty("cassandra.ring_delay_ms");
        if (newdelay != null)
        {
            logger_.warn("Overriding RING_DELAY to {}ms", newdelay);
            return Integer.parseInt(newdelay);
        }
        else
            return 30 * 1000;
    }

    /**
     * This pool is used for periodic short (sub-second) tasks.
     */
     public static final DebuggableScheduledThreadPoolExecutor scheduledTasks = new DebuggableScheduledThreadPoolExecutor("ScheduledTasks");

    /**
     * This pool is used by tasks that can have longer execution times, and usually are non periodic.
     */
    public static final DebuggableScheduledThreadPoolExecutor tasks = new DebuggableScheduledThreadPoolExecutor("NonPeriodicTasks");

    /**
     * tasks that do not need to be waited for on shutdown/drain
     */
    public static final DebuggableScheduledThreadPoolExecutor optionalTasks = new DebuggableScheduledThreadPoolExecutor("OptionalTasks");
    static
    {
        tasks.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    /** This abstraction maintains the mapping from ColumnFamily ID to token/endpoint metadata information */
    private ConcurrentHashMap<Integer, TokenMetadata> tokenMetadata_ = new ConcurrentHashMap<Integer, TokenMetadata>();
//    
//    /** This abstraction maintains the mapping from endpoint to range metadata information */
//    private Map<InetAddress, RangeMetadata> rangeMetadata_ = new HashMap<InetAddress, RangeMetadata>();
    
    private IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
    public VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
    private Double systemCpuUsage = 0.0;
    private Map<Integer, Map<Token, Double>> hitCountPerMinute = Maps.newHashMap();
    
    public static final StorageService instance = new StorageService();

    public static IPartitioner getPartitioner()
    {
        return instance.partitioner;
    }

    public Collection<Range> getLocalRanges(String table, String cfName)
    {
        return getRangesForEndpoint(table, cfName, FBUtilities.getBroadcastAddress());
    }

    public Range getLocalPrimaryRange()
    {
        return getPrimaryRangeForEndpoint(FBUtilities.getBroadcastAddress());
    }

    private Set<InetAddress> replicatingNodes = Collections.synchronizedSet(new HashSet<InetAddress>());
    private CassandraDaemon daemon;

    /* Are we starting this node in bootstrap mode? */
    private boolean isBootstrapMode;
    /* when intialized as a client, we shouldn't write to the system table. */
    private boolean isClientMode;
    private boolean initialized;
    private volatile boolean joined = false;

    private static enum Mode { NORMAL, CLIENT, JOINING, LEAVING, DECOMMISSIONED, MOVING, DRAINING, DRAINED }
    private Mode operationMode;

    private MigrationManager migrationManager = new MigrationManager();

    /* Used for tracking drain progress */
    private volatile int totalCFs, remainingCFs;

    private static final AtomicInteger nextRepairCommand = new AtomicInteger();

    public void finishBootstrapping()
    {
        isBootstrapMode = false;
    }

    public void setTokens(String keyspace, String columnfamily, Collection<Token> tokens)
    {
    	if (logger_.isDebugEnabled())
            logger_.debug("Setting tokens for (" + keyspace + ", " + columnfamily 
            		+ ") to {" + StringUtils.join(tokens, ", ") + "}");
        
        SystemTable.replaceTokensForLocalEndpoint(keyspace, columnfamily, tokens);
        TokenMetadata tm = getTokenMetadata(keyspace, columnfamily);
        tm.addNormalEndpoint(FBUtilities.getBroadcastAddress(), tokens);
        
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
        		valueFactory.normal(keyspace, columnfamily, tokens));
    }

    public StorageService()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=StorageService"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        /* register the verb handlers */
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.MUTATION, new RowMutationVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.READ_REPAIR, new ReadRepairVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.READ, new ReadVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.RANGE_SLICE, new RangeSliceVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.INDEX_SCAN, new IndexScanVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.COUNTER_MUTATION, new CounterMutationVerbHandler());
        // see BootStrapper for a summary of how the bootstrap verbs interact
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.BOOTSTRAP_TOKENS, new BootStrapper.BootstrapTokenVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.RING, new BootStrapper.RingStateVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.SPLIT, new BootStrapper.SplitRangeVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.REMOVE_TOKEN, new BootStrapper.RemoveTokenVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.REMOVING_TOKEN, new BootStrapper.RemovingTokenVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.VOLUME, new DataVolumeRequester.DataVolumeVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.SYSTEM_LOAD, new LoadRequester.SystemLoadVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.HOTSPOT, new LoadRequester.HotSpotVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.STREAM_REQUEST, new StreamRequestVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.STREAM_REPLY, new StreamReplyVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.REPLICATION_FINISHED, new ReplicationFinishedVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.REQUEST_RESPONSE, new ResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.INTERNAL_RESPONSE, new ResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.TREE_REQUEST, new TreeRequestVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.TREE_RESPONSE, new AntiEntropyService.TreeResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.STREAMING_REPAIR_REQUEST, new StreamingRepairTask.StreamingRepairRequest());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.STREAMING_REPAIR_RESPONSE, new StreamingRepairTask.StreamingRepairResponse());

        MessagingService.instance().registerVerbHandlers(StorageService.Verb.GOSSIP_DIGEST_SYN, new GossipDigestSynVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.GOSSIP_DIGEST_ACK, new GossipDigestAckVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.GOSSIP_DIGEST_ACK2, new GossipDigestAck2VerbHandler());

        MessagingService.instance().registerVerbHandlers(StorageService.Verb.DEFINITIONS_UPDATE, new DefinitionsUpdateVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.TRUNCATE, new TruncateVerbHandler());
        MessagingService.instance().registerVerbHandlers(StorageService.Verb.SCHEMA_CHECK, new SchemaCheckVerbHandler());

        // spin up the streaming serivice so it is available for jmx tools.
        if (StreamingService.instance == null)
            throw new RuntimeException("Streaming service is unavailable.");
    }

    public void registerDaemon(CassandraDaemon daemon)
    {
        this.daemon = daemon;
    }

    // should only be called via JMX
    public void stopGossiping()
    {
        if (initialized)
        {
            logger_.warn("Stopping gossip by operator request");
            Gossiper.instance.stop();
            initialized = false;
        }
    }

    // should only be called via JMX
    public void startGossiping()
    {
        if (!initialized)
        {
            logger_.warn("Starting gossip by operator request");
            Gossiper.instance.start((int)(System.currentTimeMillis() / 1000));
            initialized = true;
        }
    }

    // should only be called via JMX
    public void startRPCServer()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured RPC daemon");
        }
        daemon.startRPCServer();
    }

    public void stopRPCServer()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured RPC daemon");
        }
        daemon.stopRPCServer();
    }

    public boolean isRPCServerRunning()
    {
        if (daemon == null)
        {
            return false;
        }
        return daemon.isRPCServerRunning();
    }

    public void stopClient()
    {
        Gossiper.instance.unregister(migrationManager);
        Gossiper.instance.unregister(this);
        Gossiper.instance.stop();
        MessagingService.instance().shutdown();
        // give it a second so that task accepted before the MessagingService shutdown gets submitted to the stage (to avoid RejectedExecutionException)
        try { Thread.sleep(1000L); } catch (InterruptedException e) {}
        StageManager.shutdownNow();
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    public synchronized void initClient() throws IOException, ConfigurationException
    {
        initClient(RING_DELAY);
    }

    public synchronized void initClient(int delay) throws IOException, ConfigurationException
    {
        if (initialized)
        {
            if (!isClientMode)
                throw new UnsupportedOperationException("StorageService does not support switching modes.");
            return;
        }
        initialized = true;
        isClientMode = true;
        logger_.info("Starting up client gossip");
        setMode(Mode.CLIENT, false);
        Gossiper.instance.register(this);
        Gossiper.instance.start((int)(System.currentTimeMillis() / 1000)); // needed for node-ring gathering.
        MessagingService.instance().listen(FBUtilities.getLocalAddress());

        // sleep a while to allow gossip to warm up (the other nodes need to know about this one before they can reply).
        try
        {
            Thread.sleep(delay);
        }
        catch (Exception ex)
        {
            throw new IOError(ex);
        }
        MigrationManager.passiveAnnounce(Schema.instance.getVersion());
    }

    public synchronized void initServer() throws IOException, ConfigurationException
    {
        initServer(RING_DELAY);
    }

    public synchronized void initServer(int delay) throws IOException, ConfigurationException
    {
        logger_.info("Cassandra version: " + FBUtilities.getReleaseVersionString());
        logger_.info("Thrift API version: " + Constants.VERSION);

        if (initialized)
        {
            if (isClientMode)
                throw new UnsupportedOperationException("StorageService does not support switching modes.");
            return;
        }
        initialized = true;
        isClientMode = false;

//        if (Boolean.parseBoolean(System.getProperty("cassandra.load_ring_state", "true")))
//        {
//            logger_.info("Loading persisted ring state");
//            loadRingState();
//        }

        if (Boolean.parseBoolean(System.getProperty("cassandra.renew_counter_id", "false")))
        {
            logger_.info("Renewing local node id (as requested)");
            NodeId.renewLocalId();
        }
        
        Thread updateCpuUsage = new Thread(new WrappedRunnable() 
        {
        	@Override
        	protected void runMayThrow() throws Exception 
        	{
        		String cmd = "/usr/local/cassandra/bin/get-cpu-usage.sh";
        		ProcessBuilder pb = new ProcessBuilder(cmd.split(" "));
        		DecimalFormat twoDec = new DecimalFormat("0.00");
        		
        		systemCpuUsage = 0.0;
        		
        		logger_.info("Start getting CPU usage...");
    			Process process = pb.start();
    			DataInputStream input = new DataInputStream(process.getInputStream());
    			BufferedReader br = new BufferedReader(new InputStreamReader(input));

    			String line = null;
    			while((line=br.readLine()) != null)
    			{
    				if(line.trim().equals(""))
    					continue;
    				/** 
    				 * 0.0 <= systemCpuUsage <= 100.0
    				 */
    				Double currUsage = Double.parseDouble(line);
    				systemCpuUsage = 0.66*systemCpuUsage + 0.34*currUsage;
    				logger_.info("CPU usage is updated as " + twoDec.format(systemCpuUsage)
    						+ "%, current usage is " + twoDec.format(currUsage) + "%");
    			}

    			process.waitFor();
        	}
        });
        updateCpuUsage.start();

        // daemon threads, like our executors', continue to run while shutdown hooks are invoked
        Thread drainOnShutdown = new Thread(new WrappedRunnable()
        {
            public void runMayThrow() throws ExecutionException, InterruptedException, IOException
            {
                ThreadPoolExecutor mutationStage = StageManager.getStage(Stage.MUTATION);
                if (mutationStage.isShutdown())
                    return; // drained already

                stopRPCServer();
                optionalTasks.shutdown();
                Gossiper.instance.stop();

                // In-progress writes originating here could generate hints to be written, so shut down MessagingService
                // before mutation stage, so we can get all the hints saved before shutting down
                MessagingService.instance().shutdown();
                mutationStage.shutdown();
                mutationStage.awaitTermination(3600, TimeUnit.SECONDS);
                StorageProxy.instance.verifyNoHintsInProgress();

                List<Future<?>> flushes = new ArrayList<Future<?>>();
                for (Table table : Table.all())
                {
                    KSMetaData ksm = Schema.instance.getKSMetaData(table.name);
                    if (!ksm.durableWrites)
                    {
                        for (ColumnFamilyStore cfs : table.getColumnFamilyStores())
                        {
                            Future<?> future = cfs.forceFlush();
                            if (future != null)
                                flushes.add(future);
                        }
                    }
                }
                FBUtilities.waitOnFutures(flushes);

                CommitLog.instance.shutdownBlocking();
                
                // wait for miscellaneous tasks like sstable and commitlog segment deletion
                tasks.shutdown();
                if (!tasks.awaitTermination(1, TimeUnit.MINUTES))
                    logger_.warn("Miscellaneous task executor still busy after one minute; proceeding with shutdown");
            }
        }, "StorageServiceShutdownHook");
        Runtime.getRuntime().addShutdownHook(drainOnShutdown);

        if (Boolean.parseBoolean(System.getProperty("cassandra.join_ring", "true")))
        {
            joinTokenRing(delay);
        }
        else
        {
            logger_.info("Not joining ring as requested. Use JMX (StorageService->joinRing()) to initiate ring joining");
        }
    }

    private synchronized void loadRingState() throws UnknownHostException
    {
    	List<String> tables = Schema.instance.getNonSystemTables();
    	for (String tablename : tables)
    	{
    		Table table = Table.open(tablename);
    		Collection<ColumnFamilyStore> cfs = table.getColumnFamilyStores();
    		for(ColumnFamilyStore cf : cfs)
    		{
    			TokenMetadata tm = getTokenMetadata(cf.metadata.cfId);
    			if(tm==null)
    			{
    				logger_.debug("Column Family " + cf.columnFamily + " must have been removed. Skip it.");
    				continue;
    			}

    			Map<InetAddress, Collection<Token>> nodes = SystemTable.getTokensForNonLocalEndpoints(table.name, cf.columnFamily);
    			for(Entry<InetAddress, Collection<Token>> node : nodes.entrySet())
    			{
    				InetAddress ep = node.getKey();
    				if(ep.equals(FBUtilities.getBroadcastAddress())) // it's added by mistake
    				{
    					SystemTable.removeNonLocalEndpointFromCF(table.name, cf.columnFamily, ep);
    				}
    				else 
    				{
    					Gossiper.instance.addSavedEndpoint(ep);
    					tm.addNormalEndpoint(ep, node.getValue());
    				}
    			}
    		}
    	}
    }

	private void joinTokenRing(int delay) throws IOException, org.apache.cassandra.config.ConfigurationException
    {
        logger_.info("Starting up server gossip");
        joined = true;

        // have to start the gossip service before we can see any info on other nodes.  this is necessary
        // for bootstrap to get the load info it needs.
        // (we won't be part of the storage ring though until we add a nodeId to our state, below.)
        Gossiper.instance.register(this);
        Gossiper.instance.register(migrationManager);
        Gossiper.instance.start(SystemTable.incrementAndGetGeneration()); // needed for node-ring gathering.
        // add rpc listening info
        Gossiper.instance.addLocalApplicationState(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(DatabaseDescriptor.getRpcAddress()));
        if (null != DatabaseDescriptor.getReplaceToken())
            Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.hibernate(true));

        MessagingService.instance().listen(FBUtilities.getLocalAddress());
//        LoadBroadcaster.instance.startBroadcasting();
        MigrationManager.passiveAnnounce(Schema.instance.getVersion());
        Gossiper.instance.addLocalApplicationState(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());

        HintedHandOffManager.instance.start();

        if (DatabaseDescriptor.isAutoBootstrap()
                && DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress())
                && !SystemTable.isBootstrapped())
            logger_.info("This node will not auto bootstrap because it is configured to be a seed node.");

        // first startup is only chance to bootstrap
        if (DatabaseDescriptor.isAutoBootstrap()
        		&& !(
//        				DatabaseDescriptor.isPersistent() && //either this or the following is false
        				(DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress())
//        						|| SystemTable.isBootstrapped() 
//        						|| !Schema.instance.getNonSystemTables().isEmpty()
        				)
        		)
        ) // it's going to bootstrap from seed nodes
        {
            setMode(Mode.JOINING, "waiting for ring and schema information", true);
            // first sleep the delay to make sure we see the schema
            try
            {
                Thread.sleep(delay);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            // now if our schema hasn't matched, keep sleeping until it does
            while (!MigrationManager.isReadyForBootstrap())
            {
                setMode(Mode.JOINING, "waiting for schema information to complete", true);
                try
                {
                    Thread.sleep(delay);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
            setMode(Mode.JOINING, "schema complete, ready to bootstrap", true);

            if (logger_.isDebugEnabled())
                logger_.debug("... got ring + schema info");
            
            List<String> tables = Schema.instance.getNonSystemTables();
            for (String tablename : tables)
            {   
                setMode(Mode.JOINING, "getting bootstrap tokens", true);
            	
            	Collection<ColumnFamilyStore> cfses = null;
            	Table table = Table.open(tablename);
                cfses = table.getColumnFamilyStores();
                logger_.info("Trying to bootstrap Table " + tablename 
                		+ ", which has " + cfses.size() + " ColumnFamilyStores");
                
                for(ColumnFamilyStore cfs : cfses)
                {
                	logger_.info("Getting bootstrap tokens for " + tablename + "-" + cfs.columnFamily);
                	
                	TokenMetadata tm = getTokenMetadata(cfs.metadata.cfId);
                	if(tm==null)
                	{
                		logger_.info("Column Family " + cfs.columnFamily + " must have been removed. Skip it.");
                		continue;
                	}
                	
                	Map<InetAddress, Collection<Token>> ringState = 
                		BootStrapper.getRingStateFrom(tablename, cfs.columnFamily, DatabaseDescriptor.getSeeds());
                	
                	if(ringState==null || ringState.isEmpty())
                	{
                		throw new RuntimeException("No existing tokens seen!  Unable to bootstrap.");
                	}
                	else
                	{
                		updateNonLocalRingState(tablename, cfs.columnFamily, ringState);
                	}
                	
                	Set<Token> tokensInSSTables = Sets.newHashSet();
                	Collection<SSTableReader> sstables = cfs.getAllSSTables();
                	for(SSTableReader sstable : sstables)
                	{
                		tokensInSSTables.add(sstable.getRange().right);
                	}
                	
                	if(tokensInSSTables.isEmpty())
                	{
                		logger_.info(tablename + "-" + cfs.columnFamily + " has no file");
                		Pair<List<Token>,List<Token>> tokenPair;
                    	if(DatabaseDescriptor.isPersistent())
                    	{
//                    		tokenMap = BootStrapper.getTokensForBalancedData(tablename, cfs.columnFamily, 
//                    				tm, cfs.getReplicationStrategy());
                    		tokenPair = new Pair<List<Token>,List<Token>>(
                    				Lists.<Token>newArrayList(), Lists.<Token>newArrayList());
                    	}
                    	else {
                    		tokenPair = BootStrapper.getTokensForBalancedLoad(tablename, cfs.columnFamily, 
                    				tm, cfs.getReplicationStrategy());
                    	}
                    	
                    	List<Token> hotTokens = tokenPair.left;
                    	List<Token> coldTokens = tokenPair.right;
                    	// at least 1/5 tokens should be bootstrapped
//                    	int needed = (coldTokens.size() + hotTokens.size())/5 - hotTokens.size();
                    	
                    	logger_.info("Start to tranfer data. HotTokens=" + hotTokens.size() 
                    			+ ", ColdTokens=" + coldTokens.size());
//                    	
//                    	int numTokensToBootstrap = Math.max((coldTokens.size() + hotTokens.size())/5, hotTokens.size());
//                    	logger_.info("Number of Tokens to bootstrap: " + numTokensToBootstrap);
//                    	
                    	if(!hotTokens.isEmpty())
                    	{
                    		bootstrapByCopy(tablename, cfs.columnFamily, hotTokens);
                    		tokensInSSTables.addAll(hotTokens);
                    	}
                    	
                    	reportDiskUsage();
                    	
                        final String ksName = tablename;
                        final String cfName = cfs.columnFamily;
                        final List<Token> moveTokens = coldTokens;
//                        
//                        if(needed > 0)
//                        {
//                        	for(int i=0; i < needed; i++)
//                        	{
//                        		Token token = coldTokens.get(i);
//                        		try {
//                    				bootstrapByMove(ksName, cfName, token);
//                    			}
//                    			catch(Exception e)
//                    			{
//                    				e.printStackTrace();
//                    			}
//                        	}
//                        	moveTokens = coldTokens.subList(needed, coldTokens.size());
//                        }
//                        else
//                        {
//                        	moveTokens = coldTokens.subList(0, coldTokens.size());
//                        }
                        
                        Thread postBootstrap = new Thread(new WrappedRunnable()
                        {
                        	@Override
                        	protected void runMayThrow() throws Exception 
                        	{
                        		Thread.sleep(300*1000);
                        		
                        		logger_.info("Start adding post-bootstrap tokens");
                        		for(Token token : moveTokens)
                        		{
                        			try {
                        				bootstrapByMove(ksName, cfName, token);
                        			}
                        			catch(Exception e)
                        			{
                        				e.printStackTrace();
                        			}
                        		}
                        		logger_.info("Finish adding post-bootstrap tokens");
                        	}
                        });
                        postBootstrap.start();
                	}
                	if(!tokensInSSTables.isEmpty()) 
                		setTokens(tablename, cfs.columnFamily, tokensInSSTables);
                }
            }
            finishBootstrapping();
            assert !isBootstrapMode; // bootstrap will block until finished
        }
        else
        {
        	List<String> tables = Schema.instance.getNonSystemTables();
            for (String tablename : tables)
            {
                Table table = Table.open(tablename);
                Collection<ColumnFamilyStore> cfses = table.getColumnFamilyStores();
                for(ColumnFamilyStore cfs : cfses)
                {
                	Set<Token> tokens = Sets.newHashSet();
                	Collection<SSTableReader> sstables = cfs.getAllSSTables();
                	for(SSTableReader sstable : sstables)
                	{
                		tokens.add(sstable.getRange().right);
                	}
                	
                	if(tokens.isEmpty())
                	{
                		System.err.println(tablename + "-" + cfs.columnFamily + " has no file");
                		tokens.addAll(SystemTable.getLocalTokens(table.name, cfs.columnFamily));
                	}
                	
            		if(tokens.isEmpty())
            		{
            			// let it be. since we still have the init token.
            			TokenMetadata tm = getTokenMetadata(cfs.metadata.cfId);
            			tokens.addAll(tm.getTokens(FBUtilities.getBroadcastAddress()));
            		}
            		logger_.info("Using " + tokens.size() + " saved tokens {" 
            				+ StringUtils.join(tokens, ", ") + "}");
        			setTokens(table.name, cfs.columnFamily, tokens);
                }
            }
        }

        setMode(Mode.NORMAL, false);
        // start participating in the ring.
        SystemTable.setBootstrapped(true);
        logger_.info("Bootstrap/Replace/Move completed! Now serving reads.");
    }

    public synchronized void joinRing() throws IOException, ConfigurationException
    {
        if (!joined)
        {
            logger_.info("Joining ring by operator request");
            joinTokenRing(0);
        }
    }

    public boolean isJoined()
    {
        return joined;
    }
    
//    public void initTokensForSystemTable(Table table)
//    {
//    	if(!table.name.equals(Table.SYSTEM_TABLE))
//    		return;
//
//		Set<Token> tokens = Collections.singleton(partitioner.decorateKey(SystemTable.TOKEN).token);
//		Map<String, Collection<Token>> cfses = Maps.newHashMap();
//		
//    	for(ColumnFamilyStore cfs : table.getColumnFamilyStores())
//    	{
//    		TokenMetadata tm = getTokenMetadata(cfs.metadata.cfId);
//    		if(tm.isEmpty())
//    		{
//    			tm.addNormalEndpoint(FBUtilities.getBroadcastAddress(), tokens);
//    			cfses.put(cfs.columnFamily, tokens);
//    		}
//    	}
//    	
//    	for(ColumnFamilyStore cfs : table.getColumnFamilyStores())
//    	{
//    		SystemTable.replaceTokensForLocalEndpoint(table.name, cfs.columnFamily, tokens, false);
//    	}
//    	SystemTable.flushPartitionCF();
//    }
    
//    public void initTokenRing(ColumnFamilyStore cfs)
//    {
//		String tableName = cfs.table.name;
//		String cfName = cfs.columnFamily;
//		Collection<Token> allTokens = null;
//		
//    	try {
//			Map<String, Collection<String>> map = SystemTable.getColumnFamilies();
//			Collection<String> values = map.get(tableName);
//			
//			if(values==null || values.contains(cfName)==false)
//	    	{
//				allTokens = requestForLocalTokens(tableName, cfName);
//	    	}
//	    	else
//	    	{
//	    		allTokens = SystemTable.getLocalTokens(tableName, cfName);
//	    		if(allTokens.isEmpty())
//	    		{
//	    			allTokens = requestForLocalTokens(tableName, cfName);
//	    		}
//	    	}
//			setTokens(tableName, cfName, allTokens);
//		}
//		catch (CharacterCodingException e) 
//		{
//			e.printStackTrace();
//		}
//    }
    
    private void reportDiskUsage() 
    {
    	String dataDir = DatabaseDescriptor.getAllDataFileLocations()[0];
    	String cmd = "du -s " + dataDir;
		ProcessBuilder pb = new ProcessBuilder(cmd.split(" "));
		
		try {
			Process process = pb.start();
			DataInputStream input = new DataInputStream(process.getInputStream());
			BufferedReader br = new BufferedReader(new InputStreamReader(input));

			String line = null;
			while((line=br.readLine()) != null)
			{
				if(line.trim().equals(""))
					continue;

				String bytes = line.split("\t")[0];
				if(bytes.contains(dataDir))
					bytes = bytes.split(" ")[0];
				
				logger_.info("Current Disk Usage is " + bytes + " bytes");
				break;
			}

			process.waitFor();
		} 
		catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Collection<Token> requestForLocalTokens(String tableName, String cfName)
    {
    	Collection<Token> allTokens = null;
    	
    	Set<InetAddress> endpoints = getRingEndpoints();
		endpoints.remove(FBUtilities.getBroadcastAddress());
		Map<InetAddress, Collection<Token>> ring = BootStrapper.getRingStateFrom(
				tableName, cfName, endpoints);
		
		if(ring==null || ring.isEmpty())
		{
			Token token = partitioner.getRandomToken();
			allTokens = Collections.singleton(token);
		}
		else
		{
			updateNonLocalRingState(tableName, cfName, ring);
			TokenMetadata tm = getTokenMetadata(tableName, cfName);
			allTokens = BootStrapper.getTokens(tm);
		}
		
		return allTokens;
    }
    
    public void setStreamThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(value);
        logger_.info("setstreamthroughput: throttle set to {}", value);
    }
    
    public int getStreamThroughputMbPerSec()
    {
        return DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec();
    }

    public int getCompactionThroughputMbPerSec()
    {
        return DatabaseDescriptor.getCompactionThroughputMbPerSec();
    }

    public void setCompactionThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.setCompactionThroughputMbPerSec(value);
    }

    private void setMode(Mode m, boolean log)
    {
        setMode(m, null, log);
    }

    private void setMode(Mode m, String msg, boolean log)
    {
        operationMode = m;
        String logMsg = msg == null ? m.toString() : String.format("%s: %s", m, msg);
        if (log)
            logger_.info(logMsg);
        else
            logger_.debug(logMsg);
    }

    
    private void bootstrapByMove(String keyspace, String columnfamily, Token token) throws IOException
    {
    	TokenMetadata tokenMetadata = getTokenMetadata(keyspace, columnfamily);
    	Set<InetAddress> endpoints = tokenMetadata.getEndpoints(token);
    	if(endpoints.contains(FBUtilities.getBroadcastAddress()))
    	{
    		logger_.warn("Token " + token + " is already added. Abort bootstrap by move.");
    		return;
    	}
    	
    	List<Pair<InetAddress, Integer>> tokenNum = Lists.<Pair<InetAddress, Integer>>newArrayList();
    	for(InetAddress ep : endpoints)
    	{
    		int num = tokenMetadata.getTokens(ep).size();
    		tokenNum.add(new Pair<InetAddress, Integer>(ep, num));
    	}
    	
    	Collections.sort(tokenNum, new Comparator<Pair<InetAddress, Integer>>()
    			{
			@Override
			public int compare(Pair<InetAddress, Integer> arg0,
					Pair<InetAddress, Integer> arg1) 
			{
				return arg0.right.compareTo(arg1.right);
			}
    	});
    	
    	InetAddress targetHost = tokenNum.get(tokenNum.size()-1).left;
    	assert tokenMetadata.getEndpoints(token).contains(targetHost);
    	
    	addToken(keyspace, columnfamily, token, targetHost);
    	BootStrapper.removeTokens(keyspace, columnfamily, targetHost, Collections.singleton(token));
    }
    
    
    private void bootstrapByCopy(String keyspace, String columnfamily, List<Token> tokenList) throws IOException
    {
        isBootstrapMode = true;

        // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping
        SystemTable.replaceTokensForLocalEndpoint(keyspace, columnfamily, tokenList);
        
        // if an existing token set, then bootstrap
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
        		valueFactory.bootstrapping(keyspace, columnfamily, tokenList));
        
//        SetMultimap<InetAddress, Token> endpoints = getReverseMap(tokenToEndpoint);
//        endpoints.removeAll(FBUtilities.getBroadcastAddress());
//        
//        for(InetAddress endpoint : endpoints.keySet())
//        {
//        	Set<Token> tokenSet = endpoints.get(endpoint);
//        	BootStrapper.removingToken(keyspace, columnfamily, endpoint, tokenSet);
//        }
        
        setMode(Mode.JOINING, "sleeping " + RING_DELAY + " ms for pending range setup", true);
        try
        {
            Thread.sleep(RING_DELAY);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        
        setMode(Mode.JOINING, "Starting to bootstrap...", true);
        BootStrapper.bootstrap(keyspace, columnfamily, tokenList); // handles token update
        
        // the files should be ready by now. Start to remove the token at the destination.
//        for(InetAddress endpoint : endpoints.keySet())
//        {
//        	Set<Token> tokenSet = endpoints.get(endpoint);
//        	BootStrapper.removeTokens(keyspace, columnfamily, endpoint, tokenSet);
//        }
    }

	private SetMultimap<InetAddress, Token> getReverseMap(Map<Token, InetAddress> tokenToEndpoint) 
	{
		SetMultimap<InetAddress, Token> retMap = HashMultimap.<InetAddress, Token>create();
		for(Map.Entry<Token, InetAddress> entry : tokenToEndpoint.entrySet())
		{
			retMap.put(entry.getValue(), entry.getKey());
		}
		return retMap;
	}

	public boolean isBootstrapMode()
    {
        return isBootstrapMode;
    }
    
    public TokenMetadata getTokenMetadata(String tableName, String cfName)
	{
    	return getTokenMetadata(Schema.instance.getId(tableName, cfName));
	}

    public TokenMetadata getTokenMetadata(Integer cfId)
	{
    	if(cfId==null)
    		return null;
    	
		TokenMetadata tm = tokenMetadata_.get(cfId);
		if(tm == null)
		{
			Pair<String, String> pair = Schema.instance.getCF(cfId);
			if(pair==null)
				return null;
			
			tm = createTokenMetadata(cfId, pair.left, pair.right, true);
		}
		return tm;
	}
    
    /**
     * Make sure there is only one copy for each CF.
     * @param cfId
     * @param tableName
     * @param cfName
     * @return
     */
    public synchronized TokenMetadata createTokenMetadata(final Integer cfId, 
    		final String tableName, final String cfName, boolean initToken)
	{
    	assert cfId != null;
    	
    	if(!tokenMetadata_.containsKey(cfId))
		{
			final TokenMetadata tm = new TokenMetadata(tableName, cfName);
			TokenMetadata old = tokenMetadata_.putIfAbsent(cfId, tm);
			if(old == null && initToken)
			{
				Set<Token> tokens = Collections.singleton(partitioner.getToken(SystemTable.TOKEN));
				tm.addNormalEndpoint(FBUtilities.getBroadcastAddress(), tokens);
			}
			
			Thread updateHitCount = new Thread(new WrappedRunnable() 
	        {
	        	@Override
	        	protected void runMayThrow() throws Exception 
	        	{
	        		Thread.sleep(60*1000);
	        		ColumnFamilyStore cfs = Table.open(tableName).getColumnFamilyStore(cfId);
	        		if(cfs == null)
	        			throw new RuntimeException("Unfound ColumnFamilyStore " 
	        					+ tableName + "-" + cfName + " before updating hit count!");
	        		
	        		Map<Token, Double> map = Maps.newHashMap();
	        		hitCountPerMinute.put(cfId, map);
	        		
	        		while(true)
	        		{
	        			Map<Token, Long> counts = cfs.getAndClearHitCounts();
	        			Set<Token> tokens = tm.getTokens(FBUtilities.getBroadcastAddress());
	        			for(Token token : tokens)
	        			{
	        				long count = 0;
	        				if(counts.containsKey(token))
	        					count = counts.get(token);
	        				
	        				if(map.containsKey(token))
	        				{
	        					double cumulatedCount = map.get(token);
	        					cumulatedCount = 0.75*cumulatedCount + 0.25*count;
	        					map.put(token, cumulatedCount);
	        				}
	        				else
	        				{
	        					map.put(token, (double) count);
	        				}
	        			}
	        			
	        			Thread.sleep(60*1000);
	        		}
	        	}
	        });
			if(tableName.equals(Table.SYSTEM_TABLE)==false)
				updateHitCount.start();
		}
		
		return tokenMetadata_.get(cfId);
	}

    private void updateNonLocalRingState(String keyspace, String columnFamily,
			Map<InetAddress, Collection<Token>> ringState) 
    {
    	TokenMetadata tm = getTokenMetadata(keyspace, columnFamily);
    	if(tm==null)
    	{
    		logger_.error("(" + keyspace + ", " + columnFamily + ") is not found in the schema. " +
    				"Failed to update the ring state.");
    		return;
    	}
    	
    	ringState.remove(FBUtilities.getBroadcastAddress());
    	tm.replaceRing(ringState);
    	SystemTable.replaceTokensForColumnFamily(keyspace, columnFamily, tm.getSortedTokens());
    	
    	for(Entry<InetAddress, Collection<Token>> entry : ringState.entrySet())
    	{
    		SystemTable.replaceTokensForEndpoint(keyspace, columnFamily, 
    				entry.getValue(), entry.getKey(), false);
    	}
    	SystemTable.flushPartitionCF();
	}
    
    private Set<InetAddress> getRingEndpoints()
    {
    	Set<InetAddress> endpoints = new HashSet<InetAddress>();
    	
    	Collection<TokenMetadata> tmdSet = tokenMetadata_.values();
    	for(TokenMetadata tmd : tmdSet)
    	{
    		endpoints.addAll(tmd.getNormalEndpoints().keySet());
    	}
    	
    	return endpoints;
    }

	/**
     * for a keyspace, return the ranges and corresponding listen addresses.
     * @param keyspace
     * @return
     */
    public Map<Range, List<String>> getRangeToEndpointMap(String keyspace, String cfName)
    {
        /* All the ranges for the tokens */
        Map<Range, List<String>> map = new HashMap<Range, List<String>>();
        Set<Entry<Range, List<InetAddress>>> entrySet = getRangeToAddressMap(keyspace, cfName).entrySet();
        for (Map.Entry<Range,List<InetAddress>> entry : entrySet)
        {
            map.put(entry.getKey(), stringify(entry.getValue()));
        }
        return map;
    }

    /**
     * Return the rpc address associated with an endpoint as a string.
     * @param endpoint The endpoint to get rpc address for
     * @return
     */
    public String getRpcaddress(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
            return DatabaseDescriptor.getRpcAddress().getHostAddress();
        else if (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS) == null)
            return endpoint.getHostAddress();
        else
            return Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS).value;
    }

    /**
     * for a keyspace, return the ranges and corresponding RPC addresses for a given keyspace.
     * @param keyspace
     * @return
     */
    public Map<Range, List<String>> getRangeToRpcaddressMap(String keyspace, String cfName)
    {
        /* All the ranges for the tokens */
        Map<Range, List<String>> map = new HashMap<Range, List<String>>();
        for (Map.Entry<Range,List<InetAddress>> entry : getRangeToAddressMap(keyspace, cfName).entrySet())
        {
            List<String> rpcaddrs = new ArrayList<String>();
            for (InetAddress endpoint: entry.getValue())
            {
                rpcaddrs.add(getRpcaddress(endpoint));
            }
            map.put(entry.getKey(), rpcaddrs);
        }
        return map;
    }

    public Map<Range, List<String>> getPendingRangeToEndpointMap(String keyspace, String cfName)
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        // some people just want to get a visual representation of things. Allow null and set it to the first
//        // non-system table.
//        if (keyspace == null)
//            keyspace = Schema.instance.getNonSystemTables().get(0);
//        
//        if(cfName == null)
//        {
//        	ColumnFamilyStore cfs = (ColumnFamilyStore) Table.open(keyspace).getColumnFamilyStores().toArray()[0];
//        	cfName = cfs.getColumnFamilyName();
//        }
//
//        TokenMetadata tokenMetadata = getTokenMetadata(keyspace, cfName);
//        Map<Range, List<String>> map = new HashMap<Range, List<String>>();
//        
//        Set<Entry<Range, Collection<InetAddress>>> entrySet = tokenMetadata.getPendingRanges().entrySet();
//        
//        for (Map.Entry<Range, Collection<InetAddress>> entry : entrySet)
//        {
//            List<InetAddress> l = new ArrayList<InetAddress>(entry.getValue());
//            map.put(entry.getKey(), stringify(l));
//        }
//        return map;
    }

    public Map<Range, List<InetAddress>> getRangeToAddressMap(String keyspace, String cfName)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system table.
        if (keyspace == null)
        {
            keyspace = Schema.instance.getNonSystemTables().get(0);
        }
        if(cfName == null)
        {
        	ColumnFamilyStore cfs = (ColumnFamilyStore) Table.open(keyspace).getColumnFamilyStores().toArray()[0];
        	cfName = cfs.getColumnFamilyName();
        }

        TokenMetadata tokenMetadata = getTokenMetadata(keyspace, cfName);
        List<Range> ranges = getAllRanges(tokenMetadata.getSortedTokens());
        return constructRangeToEndpointMap(keyspace, cfName, ranges);
    }

    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the String for JMX compatibility
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given keyspace
     *
     * @throws InvalidRequestException if there is no ring information available about keyspace
     */
    public List<String> describeRingJMX(String keyspace, String cfName) throws InvalidRequestException
    {
        List<String> result = new ArrayList<String>();

        for (TokenRange tokenRange : describeRing(keyspace, cfName))
            result.add(tokenRange.toString());

        return result;
    }

    /**
     * The TokenRange for a given keyspace.
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) for the given keyspace
     *
     * @throws InvalidRequestException if there is no ring information available about keyspace
     */
    public List<TokenRange> describeRing(String keyspace, String cfName) throws InvalidRequestException
    {
        if (keyspace == null || !Schema.instance.getNonSystemTables().contains(keyspace))
            throw new InvalidRequestException("There is no ring for the keyspace: " + keyspace);
        
        if (cfName == null || Schema.instance.getId(keyspace, cfName) == null)
            throw new InvalidRequestException("There is no ring for the column family: " + cfName);

        List<TokenRange> ranges = new ArrayList<TokenRange>();
        Token.TokenFactory tf = getPartitioner().getTokenFactory();

        for (Map.Entry<Range, List<InetAddress>> entry : getRangeToAddressMap(keyspace, cfName).entrySet())
        {
            Range range = entry.getKey();
            List<String> endpoints = new ArrayList<String>();
            List<String> rpc_endpoints = new ArrayList<String>();
            List<EndpointDetails> epDetails = new ArrayList<EndpointDetails>();

            for (InetAddress endpoint : entry.getValue())
            {
                EndpointDetails details = new EndpointDetails();
                details.host = endpoint.getHostAddress();
                details.datacenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
                details.rack = DatabaseDescriptor.getEndpointSnitch().getRack(endpoint);

                endpoints.add(details.host);
                rpc_endpoints.add(getRpcaddress(endpoint));

                epDetails.add(details);
            }

            TokenRange tr = new TokenRange(tf.toString(range.left), tf.toString(range.right), endpoints)
                                    .setEndpoint_details(epDetails)
                                    .setRpc_endpoints(rpc_endpoints);

            ranges.add(tr);
        }

        return ranges;
    }

    public Map<String, List<String>> getEndpointToTokensMap(String table, String cfName)
    {
    	TokenMetadata tm = getTokenMetadata(table, cfName);
    	if(tm==null)
    		return new HashMap<String, List<String>>(0);
    	
        Map<InetAddress, Collection<Token>> normals = tm.getNormalEndpoints();
//        Map<InetAddress, Collection<Token>> bootstraps = tm.getBootstrapEndpoints();
        Map<String, List<String>> mapString = new HashMap<String, List<String>>();
        TokenFactory tf = partitioner.getTokenFactory();
        
        for (Entry<InetAddress, Collection<Token>> entry : normals.entrySet())
        {
        	String host = entry.getKey().getHostAddress();
        	List<String> tokenStr = new ArrayList<String>();
        	for(Token token : entry.getValue())
        	{
        		tokenStr.add(tf.toString(token));
        	}
        	mapString.put(host, tokenStr);
        }
//        for (Entry<InetAddress, Collection<Token>> entry : bootstraps.entrySet())
//        {
//        	mapString.putAll(entry.getKey().getHostAddress(), entry.getValue());
//        }
        
        return mapString;
    }
    
    public List<String> getSortedTokens(String table, String cfName)
    {
    	TokenMetadata tm = getTokenMetadata(table, cfName);
    	if(tm==null)
    		return new ArrayList<String>(0);
    	
    	ArrayList<Token> tokens = tm.getSortedTokens();
    	List<String> strs = new ArrayList<String>(tokens.size());
    	TokenFactory tf = partitioner.getTokenFactory();
    	
    	for(Token token : tokens)
    	{
    		strs.add(tf.toString(token));
    	}
    	return strs;
    }

    /**
     * Construct the range to endpoint mapping based on the true view
     * of the world.
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    private Map<Range, List<InetAddress>> constructRangeToEndpointMap(String keyspace, String cfName, List<Range> ranges)
    {
        Map<Range, List<InetAddress>> rangeToEndpointMap = new HashMap<Range, List<InetAddress>>();
        for (Range range : ranges)
        {
        	AbstractReplicationStrategy strategy = Table.open(keyspace).getColumnFamilyStore(cfName).getReplicationStrategy();
            rangeToEndpointMap.put(range, strategy.getNaturalEndpoints(range.right));
        }
        return rangeToEndpointMap;
    }

    /*
     * Handle the reception of a new particular ApplicationState for a particular endpoint. Note that the value of the
     * ApplicationState has not necessarily "changed" since the last known value, if we already received the same update
     * from somewhere else.
     *
     * onChange only ever sees one ApplicationState piece change at a time (even if many ApplicationState updates were
     * received at the same time), so we perform a kind of state machine here. We are concerned with two events: knowing
     * the token associated with an endpoint, and knowing its operation mode. Nodes can start in either bootstrap or
     * normal mode, and from bootstrap mode can change mode to normal. A node in bootstrap mode needs to have
     * pending ranges set in TokenMetadata; a node in normal mode should instead be part of the token ring.
     *
     * Normal progression of ApplicationState.STATUS values for a node should be like this:
     * STATUS_BOOTSTRAPPING,token
     *   if bootstrapping. stays this way until all files are received.
     * STATUS_NORMAL,token
     *   ready to serve reads and writes.
     * STATUS_LEAVING,token
     *   get ready to leave the cluster as part of a decommission
     * STATUS_LEFT,token
     *   set after decommission is completed.
     *
     * Other STATUS values that may be seen (possibly anywhere in the normal progression):
     * STATUS_MOVING,newtoken
     *   set if node is currently moving to a new token in the ring
     * REMOVING_TOKEN,deadtoken
     *   set if the node is dead and is being removed by its REMOVAL_COORDINATOR
     * REMOVED_TOKEN,deadtoken
     *   set if the node is dead and has been removed by its REMOVAL_COORDINATOR
     *
     * Note: Any time a node state changes from STATUS_NORMAL, it will not be visible to new nodes. So it follows that
     * you should never bootstrap a new node during a removetoken, decommission or move.
     */
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
//    	logger_.info("Received onChange message from " + endpoint + ", Value = " + value.value);
        switch (state)
        {
            case STATUS:
            	if(endpoint.equals(FBUtilities.getBroadcastAddress()))
            		return;
            	
                String apStateValue = value.value;
                String[] pieces = apStateValue.split(VersionedValue.DELIMITER_STR, -1);
                assert (pieces.length > 0);

                String moveName = pieces[0];

                if (moveName.equals(VersionedValue.STATUS_BOOTSTRAPPING))
                    handleStateBootstrap(endpoint, pieces);
                else if (moveName.equals(VersionedValue.STATUS_NORMAL))
                    handleStateNormal(endpoint, pieces);
                else if (moveName.equals(VersionedValue.STATUS_LEAVING))
                    handleStateLeaving(endpoint, pieces);
                else if (moveName.equals(VersionedValue.STATUS_LEFT))
                    handleStateLeft(endpoint, pieces);
                else if (moveName.equals(VersionedValue.SPLIT))
                    handleStateSplit(endpoint, pieces);
                else if (moveName.equals(VersionedValue.ADDING_TOKEN))
                    handleStateAddingToken(endpoint, pieces);
                else if (moveName.equals(VersionedValue.ADDED_TOKEN))
                    handleStateAddedToken(endpoint, pieces);
                else if (moveName.equals(VersionedValue.REMOVING_TOKEN))
                    handleStateRemovingToken(endpoint, pieces);
                else if (moveName.equals(VersionedValue.REMOVED_TOKEN))
                    handleStateRemovedToken(endpoint, pieces);
        }
    }

    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     * @param pieces STATE_BOOTSTRAPPING,bootstrap tokens as string
     */
    private void handleStateBootstrap(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 4;
        String tableName = pieces[1];
        String cfName = pieces[2];
        Collection<String> stringSet = VersionedValueFactory.fromString(pieces[3]);
        
        Set<Token> tokens = new HashSet<Token>(stringSet.size());
        TokenFactory tf = getPartitioner().getTokenFactory();
        for(String str : stringSet)
    	{
    		tokens.add(tf.fromString(str));
    	}

//        if (logger_.isDebugEnabled())
        logger_.info("Node " + endpoint + " state bootstrapping, " 
        		+ tokens.size() + " tokens. " + StringUtils.join(tokens, ",")
        		+ " for ColumnFamily " + tableName + "/" + cfName);

        Integer cfId = Schema.instance.getId(tableName, cfName);
        if(cfId == null)
        {
        	logger_.debug("The CF metadta is not loaded from other nodes yet. " +
        			"Consider to restart this node");
        	return;
        }
        
        TokenMetadata tokenMetadata = getTokenMetadata(cfId);
        // if this node is present in token metadata, either we have missed intermediate states
        // or the node had crashed. Print warning if needed, clear obsolete stuff and
        // continue.
        if (tokenMetadata.isMember(endpoint))
        {
            // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
            // isLeaving is true, we have only missed LEFT. Waiting time between completing
            // leave operation and rebootstrapping is relatively short, so the latter is quite
            // common (not enough time for gossip to spread). Therefore we report only the
            // former in the log.
//            if (!tokenMetadata.isLeaving(endpoint))
//                logger_.info("Node " + endpoint + " state jump to bootstrap");
            tokenMetadata.removeEndpoint(endpoint);
        }

        tokenMetadata.addBootstrapEndpoint(endpoint, tokens);
    }

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     * @param pieces STATE_NORMAL,token
     */
    private void handleStateNormal(InetAddress endpoint, String[] pieces)
    {
    	assert pieces.length >= 4;
        String tableName = pieces[1];
        String cfName = pieces[2];
        Collection<String> stringSet = VersionedValueFactory.fromString(pieces[3]);
        
        Set<Token> tokens = new HashSet<Token>(stringSet.size());
        TokenFactory tf = getPartitioner().getTokenFactory();
        for(String str : stringSet)
    	{
    		tokens.add(tf.fromString(str));
    	}

//        if (logger_.isDebugEnabled())
        logger_.info("Node " + endpoint + " state normal, " 
        		+ tokens.size() + " tokens. " + StringUtils.join(tokens, ",")
        		+ " for ColumnFamily " + tableName + "/" + cfName);

        Integer cfId = Schema.instance.getId(tableName, cfName);
        if(cfId == null)
        {
        	logger_.debug("The CF metadta is not loaded from other nodes yet. " +
        			"Consider to restart this node");
        	return;
        }
        
        TokenMetadata tokenMetadata = getTokenMetadata(cfId);
        
        if (tokenMetadata.isMember(endpoint))
            logger_.info("Node " + endpoint + " state jump to normal");

        if (!isClientMode)
        {
        	SystemTable.replaceTokensForEndpoint(tableName, cfName, tokens, endpoint, true);
        }
        tokenMetadata.addNormalEndpoint(endpoint, tokens);
    }

    /**
     * Handle node preparing to leave the ring
     *
     * @param endpoint node
     * @param pieces STATE_LEAVING,token
     */
    private void handleStateLeaving(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 3;
        String tableName = pieces[1];
        String cfName = pieces[2];

//        if (logger_.isDebugEnabled())
        logger_.info("Node " + endpoint + " state leaving ColumnFamily " + tableName + "/" + cfName);

        Integer cfId = Schema.instance.getId(tableName, cfName);
        if(cfId == null)
        {
        	return;
        }
        
        TokenMetadata tokenMetadata = getTokenMetadata(cfId);
        
        // If the node is previously unknown or tokens do not match, update tokenmetadata to
        // have this node as 'normal' (it must have been using this token before the
        // leave). This way we'll get pending ranges right.
        if (!tokenMetadata.isMember(endpoint))
        {
            logger_.info("Node " + endpoint + " state jump to leaving");
        }

        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        tokenMetadata.addLeavingEndpoint(endpoint);
    }

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    private void handleStateLeft(InetAddress endpoint, String[] pieces)
    {
    	assert pieces.length >= 4;
        String tableName = pieces[1];
        String cfName = pieces[2];
        Long expireTime = Long.parseLong(pieces[3]);
        
//        if (logger_.isDebugEnabled())
        logger_.info("Node " + endpoint + " state left ColumnFamily " + tableName + "/" + cfName);

        excise(tableName, cfName, endpoint, expireTime);
    }

    /**
     * Handle range split in other nodes. This happens when a RangeSplitTask is executed.
     * 
     * @param endpoint
     * @param pieces
     */
    private void handleStateSplit(InetAddress endpoint, String[] pieces)
    {
    	assert pieces.length >= 7;
    	
    	InetAddress localAddress = FBUtilities.getBroadcastAddress();
        if(endpoint.equals(localAddress))
        	return;
    	
    	String tableName = pieces[1];
        String cfName = pieces[2];
        Integer cfId = Schema.instance.getId(tableName, cfName);
        if(cfId == null)
        {
        	logger_.debug("The CF metadta is not loaded from other nodes yet. " +
        			"Consider to restart this node");
        	return;
        }
        
        TokenFactory tf = getPartitioner().getTokenFactory();
        Token left = tf.fromString(pieces[3]);
        Token right = tf.fromString(pieces[4]);
        Token newToken = tf.fromString(pieces[5]);
//        Long expireTime = Long.parseLong(pieces[6]);
        
        Range range = new Range(left, right);
        
        logger_.info("Node " + endpoint + " state range split for Range " 
        		+ range + " by " + newToken 
        		+ " in ColumnFamily " + tableName + "/" + cfName);
        
        TokenMetadata tokenMetadata = getTokenMetadata(tableName, cfName);
        Set<Token> localTokens = tokenMetadata.getTokens(localAddress);
        
        if(localTokens.contains(range.right) && !localTokens.contains(newToken))
        {
        	ColumnFamilyStore cfs = Table.open(tableName).getColumnFamilyStore(cfName);
        	addSplitToken(tableName, cfName, range, newToken);
            CompactionManager.instance.submitSplitRange(cfs, range);
        }
        else
        {
        	tokenMetadata.addSplitToken(endpoint, range, newToken);
        }
    }
    
    /**
     * Handle adding an existing token to an existing node.
     *
     * @param endpoint moving endpoint address
     * @param pieces ADDING_TOKEN, token
     */
    private void handleStateAddingToken(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 4;
        String tableName = pieces[1];
        String cfName = pieces[2];
        Token newToken = getPartitioner().getTokenFactory().fromString(pieces[3]);

//        if (logger_.isDebugEnabled())
        logger_.info("Node " + endpoint + " state adding new token " + newToken
        		+ " to ColumnFamily " + tableName + "/" + cfName);
        
        Integer cfId = Schema.instance.getId(tableName, cfName);
        if(cfId == null)
        {
        	logger_.debug("The CF metadta is not loaded from other nodes yet. " +
        			"Consider to restart this node");
        	return;
        }
        
        TokenMetadata tokenMetadata = getTokenMetadata(cfId);
        tokenMetadata.addJoiningToken(endpoint, newToken);
    }
    
    /**
     * Handle added an existing token to an existing node.
     *
     * @param endpoint moving endpoint address
     * @param pieces ADDING_TOKEN, token
     */
    private void handleStateAddedToken(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 5;
        String tableName = pieces[1];
        String cfName = pieces[2];
        Token newToken = getPartitioner().getTokenFactory().fromString(pieces[3]);
//        Long expireTime = Long.parseLong(pieces[4]);
        
//        if (logger_.isDebugEnabled())
        logger_.info("Node " + endpoint + " state added new token " + newToken
        		+ " to ColumnFamily " + tableName + "/" + cfName);
        
        Integer cfId = Schema.instance.getId(tableName, cfName);
        if(cfId == null)
        {
        	logger_.debug("The CF metadta is not loaded from other nodes yet. " +
        			"Consider to restart this node");
        	return;
        }
        
        TokenMetadata tokenMetadata = getTokenMetadata(cfId);
        tokenMetadata.addNormalToken(endpoint, newToken);
    }

    /**
     * Handle notification that a token is being removed from an endpoint
     *
     * @param endpoint node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    private void handleStateRemovingToken(InetAddress endpoint, String[] pieces)
    {
    	assert pieces.length >= 4;
        String tableName = pieces[1];
        String cfName = pieces[2];
        Token token = getPartitioner().getTokenFactory().fromString(pieces[3]);

        logger_.info("Node " + endpoint + " state removing " + token 
        		+ " from ColumnFamily " + tableName + "/" + cfName);
        
        Integer cfId = Schema.instance.getId(tableName, cfName);
        if(cfId == null)
        {
        	logger_.debug("The CF metadta is not loaded from other nodes yet. " +
        			"Consider to restart this node");
        	return;
        }
        
        TokenMetadata tokenMetadata = getTokenMetadata(cfId);
        tokenMetadata.addLeavingToken(endpoint, token);
    }
    
    /**
     * Handle notification that a token is being removed from an endpoint
     *
     * @param endpoint node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    private void handleStateRemovedToken(InetAddress endpoint, String[] pieces)
    {
    	assert pieces.length >= 5;
        String tableName = pieces[1];
        String cfName = pieces[2];
        Token token = getPartitioner().getTokenFactory().fromString(pieces[3]);
//      Long expireTime = Long.parseLong(pieces[4]);

        logger_.info("Node " + endpoint + " state removed " + token
        		+ " from ColumnFamily " + tableName + "/" + cfName);
        
        Integer cfId = Schema.instance.getId(tableName, cfName);
        if(cfId == null)
        {
        	logger_.debug("The CF metadta is not loaded from other nodes yet. " +
        			"Consider to restart this node");
        	return;
        }
        
        TokenMetadata tokenMetadata = getTokenMetadata(cfId);
        tokenMetadata.removeNormalToken(endpoint, token);
    }

    private void excise(String tableName, String cfName, InetAddress endpoint)
    {
        HintedHandOffManager.instance.deleteHintsForEndpoint(endpoint);
        Gossiper.instance.removeEndpoint(endpoint);
        
        Integer cfId = Schema.instance.getId(tableName, cfName);
        if(cfId == null)
        {
        	logger_.debug("The CF metadta is not loaded from other nodes yet. " +
        			"Consider to restart this node");
        	return;
        }
        
        TokenMetadata tokenMetadata = getTokenMetadata(cfId);
        tokenMetadata.removeEndpoint(endpoint);
//        tokenMetadata.removeBootstrapEndpoint(endpoint);
        if (!isClientMode)
        {
            logger_.info("Removing endpoint " + endpoint + " from ColumnFamily " + tableName + "/" + cfName);
            SystemTable.removeNonLocalEndpointFromCF(tableName, cfName, endpoint);
        }
    }
    
    private void excise(String tableName, String cfName, InetAddress endpoint, long expireTime)
    {
        addExpireTimeIfFound(endpoint, expireTime);
        excise(tableName, cfName, endpoint);
    }
    
    protected void addExpireTimeIfFound(InetAddress endpoint, long expireTime)
    {
        if (expireTime != 0L)
        {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }

    /**
     * Calculate pending ranges according to bootstrapping and leaving nodes. Reasoning is:
     *
     * (1) When in doubt, it is better to write too much to a node than too little. That is, if
     * there are multiple nodes moving, calculate the biggest ranges a node could have. Cleaning
     * up unneeded data afterwards is better than missing writes during movement.
     * (2) When a node leaves, ranges for other nodes can only grow (a node might get additional
     * ranges, but it will not lose any of its current ranges as a result of a leave). Therefore
     * we will first remove _all_ leaving tokens for the sake of calculation and then check what
     * ranges would go where if all nodes are to leave. This way we get the biggest possible
     * ranges with regard current leave operations, covering all subsets of possible final range
     * values.
     * (3) When a node bootstraps, ranges of other nodes can only get smaller. Without doing
     * complex calculations to see if multiple bootstraps overlap, we simply base calculations
     * on the same token ring used before (reflecting situation after all leave operations have
     * completed). Bootstrapping nodes will be added and removed one by one to that metadata and
     * checked what their ranges would be. This will give us the biggest possible ranges the
     * node could have. It might be that other bootstraps make our actual final ranges smaller,
     * but it does not matter as we can clean up the data afterwards.
     *
     * NOTE: This is heavy and ineffective operation. This will be done only once when a node
     * changes state in the cluster, so it should be manageable.
     */

    /**
     * Finds living endpoints responsible for the given ranges
     *
     * @param table the table ranges belong to
     * @param ranges the ranges to find sources for
     * @return multimap of addresses to ranges the address is responsible for
     */
    private Multimap<InetAddress, Range> getNewSourceRanges(String table, String cfName, Set<Range> ranges)
    {
        InetAddress myAddress = FBUtilities.getBroadcastAddress();
        ColumnFamilyStore cfs = Table.open(table).getColumnFamilyStore(cfName);
        TokenMetadata tokenMetadata = getTokenMetadata(cfs.metadata.cfId);
        Multimap<Range, InetAddress> rangeAddresses = cfs.getReplicationStrategy().getRangeAddresses(tokenMetadata);
        Multimap<InetAddress, Range> sourceRanges = HashMultimap.create();
        IFailureDetector failureDetector = FailureDetector.instance;

        // find alive sources for our new ranges
        for (Range range : ranges)
        {
            Collection<InetAddress> possibleRanges = rangeAddresses.get(range);
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            List<InetAddress> sources = snitch.getSortedListByProximity(myAddress, possibleRanges);

            assert (!sources.contains(myAddress));

            for (InetAddress source : sources)
            {
                if (failureDetector.isAlive(source))
                {
                    sourceRanges.put(source, range);
                    break;
                }
            }
        }
        return sourceRanges;
    }

    /**
     * Sends a notification to a node indicating we have finished replicating data.
     *
     * @param local the local address
     * @param remote node to send notification to
     */
    private void sendReplicationNotification(InetAddress local, InetAddress remote)
    {
        // notify the remote token
        Message msg = new Message(local, StorageService.Verb.REPLICATION_FINISHED, new byte[0], Gossiper.instance.getVersion(remote));
        IFailureDetector failureDetector = FailureDetector.instance;
        if (logger_.isDebugEnabled())
            logger_.debug("Notifying " + remote.toString() + " of replication completion\n");
        while (failureDetector.isAlive(remote))
        {
            IAsyncResult iar = MessagingService.instance().sendRR(msg, remote);
            try
            {
                iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
                return; // done
            }
            catch(TimeoutException e)
            {
                // try again
            }
        }
    }

    /**
     * Called when an endpoint is removed from the ring. This function checks
     * whether this node becomes responsible for new ranges as a
     * consequence and streams data if needed.
     *
     * This is rather ineffective, but it does not matter so much
     * since this is called very seldom
     *
     * @param endpoint the node that left
     */
    private void restoreReplicaCount(InetAddress endpoint, final InetAddress notifyEndpoint)
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        final Multimap<InetAddress, String> fetchSources = HashMultimap.create();
//        Multimap<String, Map.Entry<InetAddress, Collection<Range>>> rangesToFetch = HashMultimap.create();
//
//        final InetAddress myAddress = FBUtilities.getBroadcastAddress();
//
//        for (String table : Schema.instance.getNonSystemTables())
//        {
//            Multimap<Range, InetAddress> changedRanges = getChangedRangesForLeaving(table, endpoint);
//            Set<Range> myNewRanges = new HashSet<Range>();
//            for (Map.Entry<Range, InetAddress> entry : changedRanges.entries())
//            {
//                if (entry.getValue().equals(myAddress))
//                    myNewRanges.add(entry.getKey());
//            }
//            Multimap<InetAddress, Range> sourceRanges = getNewSourceRanges(table, myNewRanges);
//            for (Map.Entry<InetAddress, Collection<Range>> entry : sourceRanges.asMap().entrySet())
//            {
//                fetchSources.put(entry.getKey(), table);
//                rangesToFetch.put(table, entry);
//            }
//        }
//
//        for (final String table : rangesToFetch.keySet())
//        {
//            for (Map.Entry<InetAddress, Collection<Range>> entry : rangesToFetch.get(table))
//            {
//                final InetAddress source = entry.getKey();
//                Collection<Range> ranges = entry.getValue();
//                final Runnable callback = new Runnable()
//                {
//                    public void run()
//                    {
//                        synchronized (fetchSources)
//                        {
//                            fetchSources.remove(source, table);
//                            if (fetchSources.isEmpty())
//                                sendReplicationNotification(myAddress, notifyEndpoint);
//                        }
//                    }
//                };
//                if (logger_.isDebugEnabled())
//                    logger_.debug("Requesting from " + source + " ranges " + StringUtils.join(ranges, ", "));
//                StreamIn.requestRanges(source, table, ranges, callback, OperationType.RESTORE_REPLICA_COUNT);
//            }
//        }
    }

    // needs to be modified to accept either a table or ARS.
    private Multimap<Range, InetAddress> getChangedRangesForLeaving(String table, String cfName, InetAddress endpoint)
    {
        // First get all ranges the leaving endpoint is responsible for
        Collection<Range> ranges = getRangesForEndpoint(table, cfName, endpoint);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endpoint + " ranges [" + StringUtils.join(ranges, ", ") + "]");
        
        AbstractReplicationStrategy strategy = Table.open(table).getColumnFamilyStore(cfName).getReplicationStrategy();
        TokenMetadata tokenMetadata = getTokenMetadata(table, cfName);

        Map<Range, List<InetAddress>> currentReplicaEndpoints = new HashMap<Range, List<InetAddress>>();

        // Find (for each range) all nodes that store replicas for these ranges as well
        for (Range range : ranges)
            currentReplicaEndpoints.put(range, strategy.calculateNaturalEndpoints(range.right, tokenMetadata));

        TokenMetadata temp = tokenMetadata.cloneAfterAllLeft();

        // endpoint might or might not be 'leaving'. If it was not leaving (that is, removetoken
        // command was used), it is still present in temp and must be removed.
        if (temp.isMember(endpoint))
            temp.removeEndpoint(endpoint);

        Multimap<Range, InetAddress> changedRanges = HashMultimap.create();

        // Go through the ranges and for each range check who will be
        // storing replicas for these ranges when the leaving endpoint
        // is gone. Whoever is present in newReplicaEndpoints list, but
        // not in the currentReplicaEndpoints list, will be needing the
        // range.
        for (Range range : ranges)
        {
            Collection<InetAddress> newReplicaEndpoints = strategy.calculateNaturalEndpoints(range.right, temp);
            newReplicaEndpoints.removeAll(currentReplicaEndpoints.get(range));
            if (logger_.isDebugEnabled())
                if (newReplicaEndpoints.isEmpty())
                    logger_.debug("Range " + range + " already in all replicas");
                else
                    logger_.debug("Range " + range + " will be responsibility of " + StringUtils.join(newReplicaEndpoints, ", "));
            changedRanges.putAll(range, newReplicaEndpoints);
        }

        return changedRanges;
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet())
        {
            onChange(endpoint, entry.getKey(), entry.getValue());
        }
    }

    public void onAlive(InetAddress endpoint, EndpointState state)
    {
//        if (!isClientMode && getTokenMetadata().isMember(endpoint))
//            HintedHandOffManager.instance.scheduleHintDelivery(endpoint);
    }

    public void onRemove(InetAddress endpoint)
    {
    	for(String table : Schema.instance.getNonSystemTables())
    	{
    		Collection<ColumnFamilyStore> cfses = Table.open(table).getColumnFamilyStores();
    		for(ColumnFamilyStore cfs : cfses)
    		{
    			TokenMetadata tokenMetadata = getTokenMetadata(cfs.metadata.cfId);
    			tokenMetadata.removeEndpoint(endpoint);
    		}
    	}
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {
        MessagingService.instance().convict(endpoint);
//        for (String tableName : Schema.instance.getNonSystemTables())
//        {
//            Collection<ColumnFamilyStore> cfses = Table.open(tableName).getColumnFamilyStores();
//            for(ColumnFamilyStore cfs : cfses)
//            {
//            	onChange(endpoint, ApplicationState.STATUS, 
//                		valueFactory.left(tableName, cfs.columnFamily, Gossiper.computeExpireTime()));
//            }
//        }
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        // If we have restarted before the node was even marked down, we need to reset the connection pool
        if (state.isAlive())
            onDead(endpoint, state);
    }
    
    /**
     * Returns the token with a hit count per min
     * @param tableName
     * @param cfName
     * @return
     */
    public Map<Token, Integer> getHotSpotTokens(String tableName, String cfName)
    {
    	Integer cfId = Schema.instance.getId(tableName, cfName);
    	TokenMetadata tm = getTokenMetadata(cfId);
    	Set<Token> tokens = tm.getTokens(FBUtilities.getBroadcastAddress());
    	List<Token> tokenList = Lists.newArrayList(tokens);
    	
    	final Map<Token, Double> map = hitCountPerMinute.get(cfId);
    	if(map==null || map.isEmpty())
    		return new HashMap<Token, Integer>();
    	
    	Collections.sort(tokenList, new Comparator<Token>() 
    			{
			@Override
			public int compare(Token t1, Token t2) 
			{
				Double count1 = map.get(t1);
				if(count1==null)
				{
					map.put(t1, 0.0);
					count1 = 0.0;
				}
				
				Double count2 = map.get(t2);
				if(count2==null)
				{
					map.put(t2, 0.0);
					count2 = 0.0;
				}
				
				return count2.compareTo(count1); //desc order
			}
    	});

    	
    	Map<Token, Integer> retMap = Maps.newHashMap();
    	for(int i=0; i<tokenList.size(); i++)
    	{
    		Token token = tokenList.get(i);
    		Double count = map.get(token);
    		retMap.put(token, count.intValue());
    	}
    	
    	return retMap;
    }

    /** raw load value */
    public Double getSystemLoad()
    {
    	return systemCpuUsage;
    }
    
    /** raw load value */
    public Map<Pair<String, String>, Double> getLoad()
    {
    	List<String> tables = Schema.instance.getNonSystemTables();
    	Map<Pair<String, String>, Double> map = new HashMap<Pair<String, String>, Double>();
    	
    	for(String tablename : tables)
    	{
    		Table table = Table.open(tablename);
    		Collection<ColumnFamilyStore> cfss = table.getColumnFamilyStores();
    		for(ColumnFamilyStore cfs : cfss)
    		{
    			Pair<String, String> pair = new Pair<String, String>(table.name, cfs.columnFamily);
    			double readCount = cfs.getReadCount();
    			map.put(pair, readCount);
    		}
        }
        return map;
    }
    
    
    /** raw data volume value */
    public long getDataVolume(String tablename, String cfName)
    {
    	Table table = Table.open(tablename);
    	if(table != null)
    	{
    		ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
    		if(cfs != null)
    			return cfs.getLiveDiskSpaceUsed();
    	}
    	return 0;
    }
    
    
    /** raw data volume value */
    public Map<Pair<String, String>, Long> getDataVolume()
    {
    	List<String> tables = Schema.instance.getNonSystemTables();
    	Map<Pair<String, String>, Long> map = new HashMap<Pair<String, String>, Long>();
    	
    	for(String tablename : tables)
    	{
    		Table table = Table.open(tablename);
    		Collection<ColumnFamilyStore> cfss = table.getColumnFamilyStores();
    		for(ColumnFamilyStore cfs : cfss)
    		{
    			Pair<String, String> pair = new Pair<String, String>(table.name, cfs.columnFamily);
    			long bytes = cfs.getLiveDiskSpaceUsed();
    			map.put(pair, bytes);
    		}
        }
        return map;
    }

    public String getLoadString()
    {
    	Map<Pair<String, String>, Double> load = getLoad();
        StringBuilder sb = new StringBuilder();
        for(Entry<Pair<String, String>, Double> entry : load.entrySet())
        {
        	sb.append("[" + entry.getKey() + ":" + entry.getValue() + "]");
        }
        return sb.toString();
    }

    public Map<String, String> getLoadMap(String keyspace, String cfName)
    {
    	TokenMetadata metadata = getTokenMetadata(keyspace, cfName);
    	Map<String, String> map = new HashMap<String, String>();
    	
    	Set<InetAddress> hosts = Sets.newHashSet(metadata.getNormalEndpoints().keySet());
		// get cpu usage of each node
		Map<InetAddress, Double> systemLoads = LoadRequester.instance.getSystemCpuUsage(hosts);
		DecimalFormat twoDec = new DecimalFormat("0.00");
		
    	for (Map.Entry<InetAddress,Double> entry : systemLoads.entrySet())
    	{
    		map.put(entry.getKey().getHostAddress(), twoDec.format(entry.getValue()) + "%");
    	}
    	return map;
    }

    public final void deliverHints(String host) throws UnknownHostException
    {
        HintedHandOffManager.instance.scheduleHintDelivery(host);
    }

    public Collection<Token> getLocalTokens(String keyspace, String columnFamily)
    {
        Collection<Token> tokens = SystemTable.getLocalTokens(keyspace, columnFamily);
        assert tokens.isEmpty() == false; // should not be called before initServer sets this
        return tokens;
    }
    
    public String getTokens(String table, String cfName)
    {
    	Collection<Token> tokens = getLocalTokens(table, cfName);
    	StringBuilder sb = new StringBuilder();
    	for(Token token : tokens)
    	{
    		sb.append(token.toString() + ',');
    	}
    	if(sb.toString().endsWith(","))
    	{
    		sb.deleteCharAt(sb.length()-1);
    	}
        return sb.toString();
    }

    public String getEndpoint()
    {
    	return FBUtilities.getBroadcastAddress().getHostAddress();
    }

    public String getReleaseVersion()
    {
        return FBUtilities.getReleaseVersionString();
    }

    public List<String> getLeavingNodes()
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        return stringify(tokenMetadata_.getLeavingEndpoints());
    }

    public List<String> getMovingNodes()
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        List<String> endpoints = new ArrayList<String>();
//        for (Pair<Token, InetAddress> node : tokenMetadata_.getMovingEndpoints())
//        {
//            endpoints.add(node.right.getHostAddress());
//        }
//        return endpoints;
    }

    public List<String> getJoiningNodes()
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        return stringify(tokenMetadata_.getBootstrapEndpoints().keySet());
    }

    public List<String> getLiveNodes()
    {
        return stringify(Gossiper.instance.getLiveMembers());
    }

    public List<String> getUnreachableNodes()
    {
        return stringify(Gossiper.instance.getUnreachableMembers());
    }

    private static String getCanonicalPath(String filename)
    {
        try
        {
            return new File(filename).getCanonicalPath();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public String[] getAllDataFileLocations()
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocations();
        for (int i = 0; i < locations.length; i++)
            locations[i] = getCanonicalPath(locations[i]);
        return locations;
    }

    public String[] getAllDataFileLocationsForTable(String table)
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocationsForTable(table);
        for (int i = 0; i < locations.length; i++)
            locations[i] = getCanonicalPath(locations[i]);
        return locations;
    }

    public String getCommitLogLocation()
    {
        return getCanonicalPath(DatabaseDescriptor.getCommitLogLocation());
    }

    public String getSavedCachesLocation()
    {
        return getCanonicalPath(DatabaseDescriptor.getSavedCachesLocation());
    }

    private List<String> stringify(Iterable<InetAddress> endpoints)
    {
        List<String> stringEndpoints = new ArrayList<String>();
        for (InetAddress ep : endpoints)
        {
            stringEndpoints.add(ep.getHostAddress());
        }
        return stringEndpoints;
    }

    public int getCurrentGenerationNumber()
    {
        return Gossiper.instance.getCurrentGenerationNumber(FBUtilities.getBroadcastAddress());
    }

    public void forceTableCleanup(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        if (tableName.equals(Table.SYSTEM_TABLE))
            throw new RuntimeException("Cleanup of the system table is neither necessary nor wise");

        NodeId.OneShotRenewer nodeIdRenewer = new NodeId.OneShotRenewer();
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            cfStore.forceCleanup(nodeIdRenewer);
        }
    }

    public void scrub(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
            cfStore.scrub();
    }

    public void upgradeSSTables(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
            cfStore.sstablesRewrite();
    }

    public void forceTableCompaction(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            cfStore.forceMajorCompaction();
        }
    }

    public void invalidateKeyCaches(String tableName, String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            cfStore.invalidateKeyCache();
        }
    }

    public void invalidateRowCaches(String tableName, String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            cfStore.invalidateRowCache();
        }
    }

    /**
     * Takes the snapshot for the given tables. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param tableNames the name of the tables to snapshot; empty means "all."
     */
    public void takeSnapshot(String tag, String... tableNames) throws IOException
    {
        if (tag == null || tag.equals(""))
            throw new IOException("You must supply a snapshot name.");

        Iterable<Table> tables;
        if (tableNames.length == 0)
        {
            tables = Table.all();
        }
        else
        {
            ArrayList<Table> t = new ArrayList<Table>();
            for (String table : tableNames)
                t.add(getValidTable(table));
            tables = t;
        }

        // Do a check to see if this snapshot exists before we actually snapshot
        for (Table table : tables)
            if (table.snapshotExists(tag))
                throw new IOException("Snapshot " + tag + " already exists.");


        for (Table table : tables)
            table.snapshot(tag);
    }

    private Table getValidTable(String tableName) throws IOException
    {
        if (!Schema.instance.getTables().contains(tableName))
        {
            throw new IOException("Table " + tableName + "does not exist");
        }
        return Table.open(tableName);
    }

    /**
     * Remove the snapshot with the given name from the given tables.
     * If no tag is specified we will remove all snapshots.
     */
    public void clearSnapshot(String tag, String... tableNames) throws IOException
    {
        if(tag == null)
            tag = "";

        Iterable<Table> tables;
        if (tableNames.length == 0)
        {
            tables = Table.all();
        }
        else
        {
            ArrayList<Table> tempTables = new ArrayList<Table>();
            for(String table : tableNames)
                tempTables.add(getValidTable(table));
            tables = tempTables;
        }

        for (Table table : tables)
            table.clearSnapshot(tag);

        if (logger_.isDebugEnabled())
            logger_.debug("Cleared out snapshot directories");
    }

    public Iterable<ColumnFamilyStore> getValidColumnFamilies(String tableName, String... cfNames) throws IOException
    {
        Table table = getValidTable(tableName);

        if (cfNames.length == 0)
            // all stores are interesting
            return table.getColumnFamilyStores();

        // filter out interesting stores
        Set<ColumnFamilyStore> valid = new HashSet<ColumnFamilyStore>();
        for (String cfName : cfNames)
        {
            ColumnFamilyStore cfStore = table.getColumnFamilyStore(cfName);
            if (cfStore == null)
            {
                // this means there was a cf passed in that is not recognized in the keyspace. report it and continue.
                logger_.warn(String.format("Invalid column family specified: %s. Proceeding with others.", cfName));
                continue;
            }
            valid.add(cfStore);
        }
        return valid;
    }

    /**
     * Flush all memtables for a table and column families.
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceTableFlush(final String tableName, final String... columnFamilies)
                throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            logger_.debug("Forcing flush on keyspace " + tableName + ", CF " + cfStore.getColumnFamilyName());
            cfStore.forceBlockingFlush();
        }
    }

    /**
     * Trigger proactive repair for a table and column families.
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceTableRepair(final String tableName, final String... columnFamilies) throws IOException
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        if (Table.SYSTEM_TABLE.equals(tableName))
//            return;
//
//        Collection<Range> ranges = getLocalRanges(tableName);
//        int cmd = nextRepairCommand.incrementAndGet();
//        logger_.info("Starting repair command #{}, repairing {} ranges.", cmd, ranges.size());
//
//        List<AntiEntropyService.RepairFuture> futures = new ArrayList<AntiEntropyService.RepairFuture>(ranges.size());
//        for (Range range : ranges)
//        {
//            AntiEntropyService.RepairFuture future = forceTableRepair(range, tableName, columnFamilies);
//            futures.add(future);
//            // wait for a session to be done with its differencing before starting the next one
//            try
//            {
//                future.session.differencingDone.await();
//            }
//            catch (InterruptedException e)
//            {
//                logger_.error("Interrupted while waiting for the differencing of repair session " + future.session + " to be done. Repair may be imprecise.", e);
//            }
//        }
//
//        boolean failedSession = false;
//
//        // block until all repair sessions have completed
//        for (AntiEntropyService.RepairFuture future : futures)
//        {
//            try
//            {
//                future.get();
//            }
//            catch (Exception e)
//            {
//                logger_.error("Repair session " + future.session.getName() + " failed.", e);
//                failedSession = true;
//            }
//        }
//
//        if (failedSession)
//            throw new IOException("Repair command #" + cmd + ": some repair session(s) failed (see log for details).");
//        else
//            logger_.info("Repair command #{} completed successfully", cmd);
    }

    public void forceTableRepairPrimaryRange(final String tableName, final String... columnFamilies) throws IOException
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        if (Table.SYSTEM_TABLE.equals(tableName))
//            return;
//
//        AntiEntropyService.RepairFuture future = forceTableRepair(getLocalPrimaryRange(), tableName, columnFamilies);
//        try
//        {
//            future.get();
//        }
//        catch (Exception e)
//        {
//            logger_.error("Repair session " + future.session.getName() + " failed.", e);
//            throw new IOException("Some repair session(s) failed (see log for details).");
//        }
    }

    public AntiEntropyService.RepairFuture forceTableRepair(final Range range, final String tableName, final String... columnFamilies) throws IOException
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        ArrayList<String> names = new ArrayList<String>();
//        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
//        {
//            names.add(cfStore.getColumnFamilyName());
//        }
//
//        return AntiEntropyService.instance.submitRepairSession(range, tableName, names.toArray(new String[names.size()]));
    }

    public void forceTerminateAllRepairSessions() 
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        AntiEntropyService.instance.terminateSessions();
    }

    /* End of MBean interface methods */

    /**
     * This method returns the predecessor of the endpoint ep on the identifier
     * space.
     */
    public InetAddress getPredecessor(InetAddress ep)
    {
//        Token token = tokenMetadata_.getToken(ep);
//        return tokenMetadata_.getEndpoint(tokenMetadata_.getPredecessor(token));
    	return null;
    }

    /*
     * This method returns the successor of the endpoint ep on the identifier
     * space.
     */
    public InetAddress getSuccessor(InetAddress ep)
    {
//        Token token = tokenMetadata_.getToken(ep);
//        return tokenMetadata_.getEndpoint(tokenMetadata_.getSuccessor(token));
    	return null;
    }

    /**
     * Get the primary range for the specified endpoint.
     * @param ep endpoint we are interested in.
     * @return range for the specified endpoint.
     */
    public Range getPrimaryRangeForEndpoint(InetAddress ep)
    {
//        return tokenMetadata_.getPrimaryRangeFor(tokenMetadata_.getToken(ep));
    	return null;
    }

    /**
     * Get all ranges an endpoint is responsible for (by table)
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    Collection<Range> getRangesForEndpoint(String table, String cfName, InetAddress ep)
    {
        return Table.open(table).getColumnFamilyStore(
        		cfName).getReplicationStrategy().getAddressRanges().get(ep);
    }

    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of
     * ranges.
     * @return ranges in sorted order
    */
    public List<Range> getAllRanges(List<Token> sortedTokens)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("computing ranges for " + StringUtils.join(sortedTokens, ", "));

        if (sortedTokens.isEmpty())
            return Collections.emptyList();
        List<Range> ranges = new ArrayList<Range>();
        int size = sortedTokens.size();
        for (int i = 1; i < size; ++i)
        {
            Range range = new Range(sortedTokens.get(i - 1), sortedTokens.get(i));
            ranges.add(range);
        }
        Range range = new Range(sortedTokens.get(size - 1), sortedTokens.get(0));
        ranges.add(range);

        return ranges;
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param table keyspace name also known as table
     * @param cf Column family name
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String table, String cfName, String key)
    {
        CFMetaData cfMetaData = Schema.instance.getTableDefinition(table).cfMetaData().get(cfName);
        return getNaturalEndpoints(cfMetaData.cfId, partitioner.getToken(cfMetaData.getKeyValidator().fromString(key)));
    }
    
    public List<InetAddress> getNaturalEndpoints(String table, String cfName, ByteBuffer key)
    {
        CFMetaData cfMetaData = Schema.instance.getCFMetaData(table, cfName);
        return getNaturalEndpoints(cfMetaData.cfId, partitioner.getToken(key));
    }

    public List<InetAddress> getNaturalEndpoints(Integer cfId, ByteBuffer key)
    {
        return getNaturalEndpoints(cfId, partitioner.getToken(key));
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param token - token for which we need to find the endpoint return value -
     * the endpoint responsible for this token
     */
    public List<InetAddress> getNaturalEndpoints(Integer cfId, Token token)
    {
    	String table = Schema.instance.getCFMetaData(cfId).ksName;
        return Table.open(table).getColumnFamilyStore(cfId).getReplicationStrategy().getNaturalEndpoints(token);
    }

    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getLiveNaturalEndpoints(String table, String cfName, ByteBuffer key)
    {
        return getLiveNaturalEndpoints(table, cfName, partitioner.getToken(key));
    }

    public List<InetAddress> getLiveNaturalEndpoints(String table, String cfName, Token token)
    {
        List<InetAddress> liveEps = new ArrayList<InetAddress>();
        List<InetAddress> endpoints = Table.open(table).getColumnFamilyStore(cfName).getReplicationStrategy().getNaturalEndpoints(token);

        for (InetAddress endpoint : endpoints)
        {
            if (FailureDetector.instance.isAlive(endpoint))
                liveEps.add(endpoint);
        }

        return liveEps;
    }

    public void setLog4jLevel(String classQualifier, String rawLevel)
    {
        Level level = Level.toLevel(rawLevel);
        org.apache.log4j.Logger.getLogger(classQualifier).setLevel(level);
        logger_.info("set log level to " + level + " for classes under '" + classQualifier + "' (if the level doesn't look like '" + rawLevel + "' then log4j couldn't parse '" + rawLevel + "')");
    }

    /**
     * @return list of Tokens (_not_ keys!) breaking up the data this node is responsible for into pieces of roughly keysPerSplit
     */
    public List<Token> getSplits(String table, String cfName, Range range, int keysPerSplit)
    {
        List<Token> tokens = new ArrayList<Token>();
        // we use the actual Range token for the first and last brackets of the splits to ensure correctness
        tokens.add(range.left);

        List<DecoratedKey> keys = new ArrayList<DecoratedKey>();
        Table t = Table.open(table);
        ColumnFamilyStore cfs = t.getColumnFamilyStore(cfName);
        for (DecoratedKey sample : cfs.allKeySamples())
        {
            if (range.contains(sample.token))
                keys.add(sample);
        }
        FBUtilities.sortSampledKeys(keys, range);
        int splits = keys.size() * DatabaseDescriptor.getIndexInterval() / keysPerSplit;

        if (keys.size() >= splits)
        {
            for (int i = 1; i < splits; i++)
            {
                int index = i * (keys.size() / splits);
                tokens.add(keys.get(index).token);
            }
        }

        tokens.add(range.right);
        return tokens;
    }

    public Map<InetAddress, Collection<Token>> getRingState(String keyspace, String columnFamily)
    {
    	TokenMetadata tm = getTokenMetadata(keyspace, columnFamily);
    	if(tm != null)
    		return tm.getNormalEndpoints();
    	
    	return null;
    }
    
    /** return the whole token set of the given column family */
    public Set<Token> getBootstrapTokens(String keyspace, String columnFamily)
    {
    	TokenMetadata tm = getTokenMetadata(keyspace, columnFamily);
    	if(tm != null)
    		return tm.getTokens(FBUtilities.getBroadcastAddress());
    	
    	return null;
    	
//    	Range range = getLocalPrimaryRange();
//        List<DecoratedKey> keys = new ArrayList<DecoratedKey>();
//        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
//        {
//            if (cfs.table.name.equals(Table.SYSTEM_TABLE))
//                continue;
//            for (DecoratedKey key : cfs.allKeySamples())
//            {
//                if (range.contains(key.token))
//                    keys.add(key);
//            }
//        }
//        FBUtilities.sortSampledKeys(keys, range);
//
//        Token token;
//        if (keys.size() < 3)
//        {
//            token = partitioner.midpoint(range.left, range.right);
//            logger_.debug("Used midpoint to assign token " + token);
//        }
//        else
//        {
//            token = keys.get(keys.size() / 2).token;
//            logger_.debug("Used key sample of size " + keys.size() + " to assign token " + token);
//        }
////        if (tokenMetadata_.getEndpoints(token) != null && tokenMetadata_.isMember(tokenMetadata_.getEndpoints(token)))
////            throw new RuntimeException("Chose token " + token + " which is already in use by " + tokenMetadata_.getEndpoints(token) + " -- specify one manually with initial_token");
////        // Hack to prevent giving nodes tokens with DELIMITER_STR in them (which is fine in a row key/token)
//        if (token instanceof StringToken)
//        {
//            token = new StringToken(((String)token.token).replaceAll(VersionedValue.DELIMITER_STR, ""));
//            if (tokenMetadata_.getNormalAndBootstrappingTokenToEndpointsMap().containsKey(token))
//                throw new RuntimeException("Unable to compute unique token for new node -- specify one manually with initial_token");
//        }
//        Set<Token> set = new HashSet<Token>();
//        set.add(token);
//        return set;
    }

    /**
     * Broadcast leaving status and update local tokenMetadata_ accordingly
     */
    private void startLeaving()
    {
        for (String tableName : Schema.instance.getNonSystemTables())
        {
            Collection<ColumnFamilyStore> cfses = Table.open(tableName).getColumnFamilyStores();
            for(ColumnFamilyStore cfs : cfses)
            {
            	Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
            			valueFactory.leaving(tableName, cfs.columnFamily));
            	
            	TokenMetadata tokenMetadata = getTokenMetadata(cfs.metadata.cfId);
            	tokenMetadata.addLeavingEndpoint(FBUtilities.getBroadcastAddress());
            }
        }
    }

    public void decommission() throws InterruptedException
    {
    	logger_.info("DECOMMISSIONING");
        
        startLeaving();
        setMode(Mode.LEAVING, "sleeping " + RING_DELAY + " ms for pending range setup", true);
        Thread.sleep(RING_DELAY);

        Runnable finishLeaving = new Runnable()
        {
            public void run()
            {
                Gossiper.instance.stop();
                MessagingService.instance().shutdown();
                StageManager.shutdownNow();
                setMode(Mode.DECOMMISSIONED, true);
                // let op be responsible for killing the process
            }
        };
        unbootstrap(finishLeaving);
    }

    private void leaveRing()
    {
        SystemTable.setBootstrapped(false);
        for (String tableName : Schema.instance.getNonSystemTables())
        {
            Collection<ColumnFamilyStore> cfses = Table.open(tableName).getColumnFamilyStores();
            for(ColumnFamilyStore cfs : cfses)
            {
            	Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
            			valueFactory.left(tableName, cfs.columnFamily, Gossiper.computeExpireTime()));
            	TokenMetadata tokenMetadata = getTokenMetadata(cfs.metadata.cfId);
            	tokenMetadata.removeEndpoint(FBUtilities.getBroadcastAddress());
            }
        }
        
        logger_.info("Announcing that I have left the ring for " + RING_DELAY + "ms");
        try
        {
            Thread.sleep(RING_DELAY);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    private void unbootstrap(final Runnable onFinish)
    {
        for(String tableName : Schema.instance.getNonSystemTables())
        {
            Collection<ColumnFamilyStore> cfses = Table.open(tableName).getColumnFamilyStores();
            for(ColumnFamilyStore cfs : cfses)
            {
            	setMode(Mode.LEAVING, "streaming data for " + tableName + "/" + cfs.columnFamily 
            			+ " to other nodes", true);
                logger_.debug("waiting for stream aks.");
            	BootStrapper.unbootstrap(tableName, cfs);
            }
        }
        
        logger_.debug("stream acks all received.");
        leaveRing();
        onFinish.run();
    }

    public void addToken(String tableName, String cfName, String tokenString) throws IOException, InterruptedException, ConfigurationException
    {
    	TokenFactory tf = partitioner.getTokenFactory();
    	tf.validate(tokenString);
    	Token newToken = tf.fromString(tokenString);
        addToken(tableName, cfName, newToken, null);
    }
    
    /**
     * This method adds an existing token to localhost.
     * 
     * @param tableName
     * @param cfName
     * @param newToken
     * @throws IOException
     */
    private void addToken(String tableName, String cfName, Token newToken, InetAddress toRemove) throws IOException
    {
    	if (newToken==null)
    	{
    		logger_.error("Can't add the undefined (null) token.");
    		return;
    	}
    	
    	CFMetaData cfmd = Schema.instance.getCFMetaData(tableName, cfName);
    	if(cfmd == null)
    	{
    		logger_.error("Can't find the undefined (null) column family.");
    		return;
    	}
    	
    	TokenMetadata tm = getTokenMetadata(cfmd.cfId);
    	if(!tm.getSortedTokens().contains(newToken))
    	{
    		logger_.error("Token " + newToken + " is not found in the ring. Abort.");
    		return;
    	}
    	
    	// address of the current node
    	InetAddress localAddress = FBUtilities.getBroadcastAddress();
    	Set<Token> localTokens = tm.getTokens(localAddress);
    	
    	if(localTokens.contains(newToken))
    	{
    		logger_.warn("Token " + newToken + " has been added already.");
    		return;
    	}
    	
    	Range range = tm.getPrimaryRangeFor(newToken);
    	// setting 'adding' application state
    	Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
    			valueFactory.addingToken(tableName, cfName, newToken));
    	logger_.info(String.format("Adding {%s} to %s.", newToken, localAddress));
    	
    	Multimap<InetAddress, Range> rangesToFetch;
    	if(toRemove != null)
    	{
    		BootStrapper.removingTokens(tableName, cfName, toRemove, Collections.singleton(newToken));
    		rangesToFetch = ArrayListMultimap.create();
    		rangesToFetch.put(toRemove, range);
    	}
    	else
    	{
    		// request data from other nodes
        	IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        	ColumnFamilyStore cfs = Table.open(tableName).getColumnFamilyStore(cfmd.cfId);
        	
        	List<InetAddress> hosts = cfs.getReplicationStrategy().calculateNaturalEndpoints(newToken, tm);
        	// get cpu usage of each node
    		Map<InetAddress, Double> systemLoads = LoadRequester.instance.getSystemCpuUsage(hosts);
    		List<InetAddress> preferred = BootStrapper.getPriorBootstrapSourcesByLoad(systemLoads);
        	
        	Multimap<Range, InetAddress> rangesToFetchWithPreferredEndpoints = ArrayListMultimap.create();
        	rangesToFetchWithPreferredEndpoints.putAll(range, preferred);
        	rangesToFetch = BootStrapper.getWorkMap(rangesToFetchWithPreferredEndpoints);
    	}
    	
    	if (!rangesToFetch.isEmpty())
    	{
    		logger_.info("Sleeping " + RING_DELAY + " ms before start fetching ranges.");

    		try
    		{
    			Thread.sleep(RING_DELAY);
    		}
    		catch (InterruptedException e)
    		{
    			throw new RuntimeException("Sleep interrupted " + e.getMessage());
    		}

    		setMode(Mode.MOVING, "fetching new ranges", true);

    		if (logger_.isDebugEnabled())
    			logger_.debug("[Move->FETCHING] Ranges: " + rangesToFetch);

    		BootStrapper.requestRanges(tableName, cfName, rangesToFetch);
    	}

        SystemTable.addTokenToLocalEndpoint(tableName, cfName, newToken);
        tm.addNormalToken(FBUtilities.getBroadcastAddress(), newToken);

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
        		valueFactory.addedToken(tableName, cfName, newToken, Gossiper.computeExpireTime()));
    	
//    	if (logger_.isDebugEnabled())
        logger_.info("Successfully added new token " + newToken);
    }

    public void addSSTable(String tableName, String cfName, Range range) throws IOException
    {
    	if (range==null)
    	{
    		logger_.error("Can't add SSTable with the undefined (null) range.");
    		return;
    	}
    	
    	CFMetaData cfmd = Schema.instance.getCFMetaData(tableName, cfName);
    	if(cfmd == null)
    	{
    		logger_.error("Can't find the undefined (null) column family.");
    		return;
    	}
    	
    	TokenMetadata tm = getTokenMetadata(cfmd.cfId);
    	
    	// address of the current node
    	InetAddress localAddress = FBUtilities.getBroadcastAddress();
    	Set<Token> localTokens = tm.getTokens(localAddress);
    	Token newToken = range.right;
    	
    	if(localTokens.contains(newToken))
    	{
    		logger_.debug("Range " + range + " is already handled by " + localAddress);
    		return;
    	}
    	
        tm.addJoiningToken(FBUtilities.getBroadcastAddress(), newToken);

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
        		valueFactory.addingToken(tableName, cfName, newToken));
//    	
//    	if (logger_.isDebugEnabled())
//    		logger_.debug("Successfully add new token {%s}", newToken);
    }
    
    public void dropColumnFamily(ColumnFamilyStore cfs)
    {
    	assert cfs != null;
    	String tableName = cfs.table.name;
    	
    	tokenMetadata_.remove(cfs.metadata.cfId);
    	SystemTable.removeColumnFamily(tableName, cfs.columnFamily);
    }
    
    public boolean isSplittable(ColumnFamilyStore cfs, Range range)
	{
		TokenMetadata tm = getTokenMetadata(cfs.metadata.cfId);
    	return tm.isSplittable(range);
	}
    
//    public Token findAvailableSplitToken(ColumnFamilyStore cfs, Range range)
//	{
//		TokenMetadata tm = getTokenMetadata(cfs.metadata.cfId);
//    	return tm.findAvailableSplitToken(range);
//	}
    
    public Token getSuccessorTokenFor(ColumnFamilyStore cfs, Token token)
	{
		TokenMetadata tm = getTokenMetadata(cfs.metadata.cfId);
    	return tm.getSuccessor(token);
	}
    
    public Token getSplitTokenFor(String tableName, String cfName, Range range, Token newToken)
    {
    	TokenMetadata tm = getTokenMetadata(tableName, cfName);
    	if(!tm.isSplittable(range))
    	{
    		logger_.info("hli@cse\tAttempt to insert new " + newToken + " for Range " + 
        			range + " proposed by other nodes");
        	
        	// no matter successful or not
        	addSplitToken(tableName, cfName, range, newToken);
    	}
    	
    	// Thread-safe: always get the token from tokenMetadata.
    	Token token = tm.findAvailableSplitToken(range);
		assert token != null;
		return token;
    }
    
    public boolean addSplitToken(ColumnFamilyStore cfs, Range range, Token newToken)
    {
    	return addSplitToken(cfs.table.name, cfs.columnFamily, range, newToken);
    }
    
    /**
     * Make sure there is only one token between the range. Should be thread safe.
     * @param tableName
     * @param cfName
     * @param range
     * @param newToken
     */
    private boolean addSplitToken(String tableName, String cfName, Range range, Token newToken)
    {
    	assert newToken != null;
    	
    	TokenMetadata tm = getTokenMetadata(tableName, cfName);
    	if(tm.isSplittable(range))
    		return false;
    	
    	InetAddress localAddress = FBUtilities.getBroadcastAddress();
    	Set<InetAddress> endpoints = tm.getEndpoints(range.right);
    	assert endpoints.contains(localAddress);
    	
    	List<InetAddress> epSortedList = new ArrayList<InetAddress>(endpoints);
    	Collections.sort(epSortedList, new Comparator<InetAddress>()
    			{
					@Override
					public int compare(InetAddress arg0, InetAddress arg1) 
					{
						return arg0.getHostAddress().compareTo(arg1.getHostAddress());
					}
    			});
    	
    	// the first node in the list makes the decision
    	if(epSortedList.get(0).equals(localAddress))
    	{
    		if(!tm.addUniqueSplitToken(localAddress, range, newToken))
        	{
        		logger_.warn(newToken + " is not unique in the given range. It will not be added.");
        		return false;
        	}
    	}
    	else
    	{
    		newToken = BootStrapper.getTokenForSplitting(epSortedList.get(0), 
    				tableName, cfName, range, newToken);
    		assert newToken != null;
    		
    		if(!tm.addUniqueSplitToken(localAddress, range, newToken))
        	{
    			newToken = tm.findAvailableSplitToken(range);
    			assert newToken != null;
        	}
    	}

    	// setting 'adding' application state
    	Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
    			valueFactory.splitRange(tableName, cfName, range, newToken, Gossiper.computeExpireTime()));

    	Set<Pair<InetAddress, Token>> inserted = tm.addSplitToken(localAddress, range, newToken);
    	
    	if(!isClientMode)
		{
    		SystemTable.addTokenToLocalEndpoint(tm.keyspace, tm.columnfamily, newToken);
			for(Pair<InetAddress, Token> pair : inserted)
			{
				if(pair.left.equals(localAddress))
				{
					SystemTable.addTokenToLocalEndpoint(tm.keyspace, tm.columnfamily, pair.right);
				}
				else
				{
					SystemTable.addTokenToNonLocalEndpoint(tm.keyspace, tm.columnfamily, pair.left, pair.right);
				}
			}
		}

    	logger_.info(String.format("hli@cse\tRange {%s} is split by new %s.", range, newToken));
    	
    	return true;
    }
    
    /**
     * Get the status of a token removal.
     */
    public String getRemovalStatus()
    {
//        if (removingNode == null) {
//            return "No token removals in process.";
//        }
//        return String.format("Removing tokens (%s). Waiting for replication confirmation from [%s].",
//        		StringUtils.join(tokenMetadata_.getTokens(removingNode), ","),
//        		StringUtils.join(replicatingNodes, ","));
    	throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Force a remove operation to complete. This may be necessary if a remove operation
     * blocks forever due to node/stream failure. removeToken() must be called
     * first, this is a last resort measure.  No further attempt will be made to restore replicas.
     */
    public void forceRemoveCompletion()
    {
    	throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Remove a node that has died, attempting to restore the replica count.
     * If the node is alive, decommission should be attempted.  If decommission
     * fails, then removeToken should be called.  If we fail while trying to
     * restore the replica count, finally forceRemoveCompleteion should be
     * called to forcibly remove the node without regard to replica count.
     *
     * @param tokenString token for the node
     */
    /**
     * remove a token from localhost. The token should still be held by other nodes
     */
    public void removeToken(String tableName, String cfName, Token token)
    {
    	CFMetaData cfmd = Schema.instance.getCFMetaData(tableName, cfName);
    	if(cfmd == null)
    	{
    		logger_.error("Can't find the undefined (null) column family.");
    		return;
    	}
    	
    	InetAddress myAddress = FBUtilities.getBroadcastAddress();
    	TokenMetadata tm = getTokenMetadata(cfmd.cfId);
    	
//    	if(!tm.getTokens(myAddress).contains(token))
//    	{
//    		logger_.error("This host (" + myAddress + ") does not have Token " + token);
//    		return;
//    	}
//
//    	Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
//    			valueFactory.removingToken(tableName, cfName, token));
//    	tm.addLeavingToken(myAddress, token);
//    	
//    	ColumnFamilyStore cfs = Table.open(tableName).getColumnFamilyStore(cfName);
//    	AbstractReplicationStrategy strategy = cfs.getReplicationStrategy();
//    	List<InetAddress> currentEndpoints = strategy.calculateNaturalEndpoints(token, tm);
//    	
//    	if(currentEndpoints.size() <= strategy.getReplicationFactor())
//    	{
//    		Set<InetAddress> endpoints = new HashSet<InetAddress>(tm.getNormalEndpoints().keySet());
//    		endpoints.removeAll(currentEndpoints);
//    		
//    		if(!endpoints.isEmpty())
//    		{
//    			// calculating endpoints to stream current ranges to if needed
//    			// in some situations node will handle current ranges as part of the new ranges
//    			Multimap<Range, InetAddress> rangesToStream = HashMultimap.create();
//    			Range range = tm.getPrimaryRangeFor(token);
//    			
//    			List<InetAddress> newEndpoints = BootStrapper.getPreferredEndpointsForMigration(
//    					endpoints, DataVolumeBroadcaster.instance.getVolumeInfo(cfmd.cfId));
//    			
//    			int numNewEndpoints = Math.min(newEndpoints.size(), 
//    					strategy.getReplicationFactor() - currentEndpoints.size() - 1);
//    			rangesToStream.putAll(range, newEndpoints.subList(0, numNewEndpoints));
//
//    	    	if (!rangesToStream.isEmpty())
//    	    	{
//    	    		logger_.info("Sleeping {%d} ms before start streaming/fetching ranges.", RING_DELAY);
//
//    	    		try
//    	    		{
//    	    			Thread.sleep(RING_DELAY);
//    	    		}
//    	    		catch (InterruptedException e)
//    	    		{
//    	    			throw new RuntimeException("Sleep interrupted " + e.getMessage());
//    	    		}
//
//    	    		setMode(Mode.MOVING, "streaming old ranges", true);
//
//    	    		if (logger_.isDebugEnabled())
//    	    			logger_.debug("[Move->STREAMING] Work Map: " + rangesToStream);
//
//    	    		CountDownLatch streamLatch = streamRanges(cfs, rangesToStream);
//
//    	    		try
//    	    		{
//    	    			streamLatch.await();
//    	    		}
//    	    		catch (InterruptedException e)
//    	    		{
//    	    			throw new RuntimeException("Interrupted latch while waiting for stream ranges to finish: " + e.getMessage());
//    	    		}
//    	    	}
//    		}
//    	}
//    	if (logger_.isDebugEnabled())
//    		logger_.debug(String.format("Removing Tokens {%s} from %s.", token, myAddress));
    	
        tm.removeNormalToken(FBUtilities.getBroadcastAddress(), token);
        
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
        		valueFactory.removedToken(tableName, cfName, token, Gossiper.computeExpireTime()));
        
        try
        {
            Thread.sleep(RING_DELAY);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    	
        ColumnFamilyStore cfs = Table.open(tableName).getColumnFamilyStore(cfName);
        logger_.info("Flushing memtables for {}...", cfs);
        Future<?> flush = cfs.forceFlush();
        if (flush != null)
        	FBUtilities.waitOnFutures(Collections.<Future<?>>singleton(flush));
        
        SystemTable.removeTokenFromLocalEndpoint(tableName, cfName, token);
        Range range = tm.getPrimaryRangeFor(token);
    	cfs.removeSSTables(range);
        
        logger_.info("Successfully removed token " + token);
    }

    public void removingToken(String tableName, String cfName, Token token) 
    {
    	CFMetaData cfmd = Schema.instance.getCFMetaData(tableName, cfName);
    	if(cfmd == null)
    	{
    		logger_.error("Can't find the undefined (null) column family.");
    		return;
    	}
    	
    	TokenMetadata tm = getTokenMetadata(cfmd.cfId);
    	tm.addLeavingToken(FBUtilities.getBroadcastAddress(), token);
        
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, 
        		valueFactory.removingToken(tableName, cfName, token));
	}

	public void confirmReplication(InetAddress node)
    {
        // replicatingNodes can be empty in the case where this node used to be a removal coordinator,
        // but restarted before all 'replication finished' messages arrived. In that case, we'll
        // still go ahead and acknowledge it.
        if (!replicatingNodes.isEmpty())
        {
            replicatingNodes.remove(node);
        }
        else
        {
            logger_.info("Received unexpected REPLICATION_FINISHED message from " + node
                         + ". Was this node recently a removal coordinator?");
        }
    }

    public boolean isClientMode()
    {
        return isClientMode;
    }

    public synchronized void requestGC()
    {
        if (hasUnreclaimedSpace())
        {
            logger_.info("requesting GC to free disk space");
            System.gc();
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    private boolean hasUnreclaimedSpace()
    {
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            if (cfs.hasUnreclaimedSpace())
                return true;
        }
        return false;
    }

    public String getOperationMode()
    {
        return operationMode.toString();
    }

    public String getDrainProgress()
    {
        return String.format("Drained %s/%s ColumnFamilies", remainingCFs, totalCFs);
    }

    /**
     * Shuts node off to writes, empties memtables and the commit log.
     * There are two differences between drain and the normal shutdown hook:
     * - Drain waits for in-progress streaming to complete
     * - Drain flushes *all* columnfamilies (shutdown hook only flushes non-durable CFs)
     */
    public synchronized void drain() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService mutationStage = StageManager.getStage(Stage.MUTATION);
        if (mutationStage.isTerminated())
        {
            logger_.warn("Cannot drain node (did it already happen?)");
            return;
        }
        setMode(Mode.DRAINING, "starting drain process", true);
        stopRPCServer();
        optionalTasks.shutdown();
        Gossiper.instance.stop();

        setMode(Mode.DRAINING, "shutting down MessageService", false);
        MessagingService.instance().shutdown();
        setMode(Mode.DRAINING, "waiting for streaming", false);
        MessagingService.instance().waitForStreaming();

        setMode(Mode.DRAINING, "clearing mutation stage", false);
        mutationStage.shutdown();
        mutationStage.awaitTermination(3600, TimeUnit.SECONDS);

        StorageProxy.instance.verifyNoHintsInProgress();

        setMode(Mode.DRAINING, "flushing column families", false);
        List<ColumnFamilyStore> cfses = new ArrayList<ColumnFamilyStore>();
        for (String tableName : Schema.instance.getNonSystemTables())
        {
            Table table = Table.open(tableName);
            cfses.addAll(table.getColumnFamilyStores());
        }
        totalCFs = remainingCFs = cfses.size();
        for (ColumnFamilyStore cfs : cfses)
        {
            cfs.forceBlockingFlush();
            remainingCFs--;
        }

        ColumnFamilyStore.postFlushExecutor.shutdown();
        ColumnFamilyStore.postFlushExecutor.awaitTermination(60, TimeUnit.SECONDS);

        CommitLog.instance.shutdownBlocking();

        // wait for miscellaneous tasks like sstable and commitlog segment deletion
        tasks.shutdown();
        if (!tasks.awaitTermination(1, TimeUnit.MINUTES))
            logger_.warn("Miscellaneous task executor still busy after one minute; proceeding with shutdown");

        setMode(Mode.DRAINED, true);
    }

    // Never ever do this at home. Used by tests.
    IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner oldPartitioner = partitioner;
        partitioner = newPartitioner;
        valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
        return oldPartitioner;
    }

    TokenMetadata setTokenMetadataUnsafe(String tableName, String cfName, TokenMetadata tmd)
    {
    	Integer cfId = Schema.instance.getId(tableName, cfName);
        TokenMetadata old = getTokenMetadata(cfId);
        if(cfId != null)
        	tokenMetadata_.put(cfId, tmd);
        
        return old;
    }

    public void truncate(String keyspace, String columnFamily) throws UnavailableException, TimeoutException, IOException
    {
        StorageProxy.truncateBlocking(keyspace, columnFamily);
    }

    public void saveCaches() throws ExecutionException, InterruptedException
    {
        List<Future<?>> futures = new ArrayList<Future<?>>();
        logger_.debug("submitting cache saves");
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            futures.add(cfs.keyCache.submitWrite(-1));
            futures.add(cfs.rowCache.submitWrite(cfs.getRowCacheKeysToSave()));
        }
        FBUtilities.waitOnFutures(futures);
        logger_.debug("cache saves completed");
    }

    public Map<Token, Float> getOwnership()
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        return partitioner.describeOwnership(getSortedTokens());
    }

    public List<String> getKeyspaces()
    {
        List<String> tableslist = new ArrayList<String>(Schema.instance.getTables());
        return Collections.unmodifiableList(tableslist);
    }

    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ConfigurationException
    {
        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();

        // new snitch registers mbean during construction
        IEndpointSnitch newSnitch = FBUtilities.construct(epSnitchClassName, "snitch");
        if (dynamic)
        {
            DatabaseDescriptor.setDynamicUpdateInterval(dynamicUpdateInterval);
            DatabaseDescriptor.setDynamicResetInterval(dynamicResetInterval);
            DatabaseDescriptor.setDynamicBadnessThreshold(dynamicBadnessThreshold);
            newSnitch = new DynamicEndpointSnitch(newSnitch);
        }

        // point snitch references to the new instance
        DatabaseDescriptor.setEndpointSnitch(newSnitch);
        for (String ks : Schema.instance.getTables())
        {
            Collection<ColumnFamilyStore> cfses = Table.open(ks).getColumnFamilyStores();
            for(ColumnFamilyStore cfs : cfses)
            {
            	cfs.getReplicationStrategy().snitch = newSnitch;
            }
        }

        if (oldSnitch instanceof DynamicEndpointSnitch)
            ((DynamicEndpointSnitch)oldSnitch).unregisterMBean();
    }

    /**
     * Flushes the two largest memtables by ops and by throughput
     */
    public void flushLargestMemtables()
    {
        ColumnFamilyStore largest = null;
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            long total = cfs.getTotalMemtableLiveSize();

            if (total > 0 && (largest == null || total > largest.getTotalMemtableLiveSize()))
            {
                logger_.debug(total + " estimated memtable size for " + cfs);
                largest = cfs;
            }
        }
        if (largest == null)
        {
            logger_.info("Unable to reduce heap usage since there are no dirty column families");
            return;
        }

        logger_.warn("Flushing " + largest + " to relieve memory pressure");
        largest.forceFlush();
    }

    public void reduceCacheSizes()
    {
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            cfs.reduceCacheSizes();
    }

    /**
     * Seed data to the endpoints that will be responsible for it at the future
     *
     * @param rangesToStreamByTable tables and data ranges with endpoints included for each
     * @return latch to count down
     */
    private CountDownLatch streamRanges(final ColumnFamilyStore cfs, final Multimap<Range, InetAddress> rangesWithEndpoints)
    {
        final CountDownLatch latch = new CountDownLatch(rangesWithEndpoints.keySet().size());

        for (final Range range : rangesWithEndpoints.keySet())
        {
        	Collection<InetAddress> endpoints = rangesWithEndpoints.get(range);
        	final Set<InetAddress> pending = new HashSet<InetAddress>(endpoints);
        	
        	for(final InetAddress newEndpoint : endpoints)
        	{
        		final Runnable callback = new Runnable()
                {
                    public void run()
                    {
                        synchronized (pending)
                        {
                            pending.remove(newEndpoint);

                            if (pending.isEmpty())
                                latch.countDown();
                        }
                    }
                };

                StageManager.getStage(Stage.STREAM).execute(new Runnable()
                {
                    public void run()
                    {
                        // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
                        StreamOut.transferRanges(newEndpoint, cfs, Arrays.asList(range), callback, OperationType.UNBOOTSTRAP);
                    }
                });
        	}
        }
        return latch;
    }



    // see calculateStreamAndFetchRanges(Iterator, Iterator) for description
    private Pair<Set<Range>, Set<Range>> calculateStreamAndFetchRanges(Collection<Range> current, Collection<Range> updated)
    {
        return calculateStreamAndFetchRanges(current.iterator(), updated.iterator());
    }

    /**
     * Calculate pair of ranges to stream/fetch for given two range collections
     * (current ranges for table and ranges after move to new token)
     *
     * @param current collection of the ranges by current token
     * @param updated collection of the ranges after token is changed
     * @return pair of ranges to stream/fetch for given current and updated range collections
     */
    private Pair<Set<Range>, Set<Range>> calculateStreamAndFetchRanges(Iterator<Range> current, Iterator<Range> updated)
    {
        Set<Range> toStream = new HashSet<Range>();
        Set<Range> toFetch  = new HashSet<Range>();

        while (current.hasNext() && updated.hasNext())
        {
            Range r1 = current.next();
            Range r2 = updated.next();

            // if ranges intersect we need to fetch only missing part
            if (r1.intersects(r2))
            {
                // adding difference ranges to fetch from a ring
                toFetch.addAll(r1.differenceToFetch(r2));

                // if current range is a sub-range of a new range we don't need to seed
                // otherwise we need to seed parts of the current range
                if (!r2.contains(r1))
                {
                    // (A, B] & (C, D]
                    if (Range.compare(r1.left, r2.left) < 0) // if A < C
                    {
                        toStream.add(new Range(r1.left, r2.left)); // seed (A, C]
                    }

                    if (Range.compare(r1.right, r2.right) > 0) // if B > D
                    {
                        toStream.add(new Range(r2.right, r1.right)); // seed (D, B]
                    }
                }
            }
            else // otherwise we need to fetch whole new range
            {
                toStream.add(r1); // should seed whole old range
                toFetch.add(r2);
            }
        }

        return new Pair<Set<Range>, Set<Range>>(toStream, toFetch);
    }

    public void bulkLoad(String directory)
    {
    	throw new UnsupportedOperationException("Not Implemented");
//        File dir = new File(directory);
//
//        if (!dir.exists() || !dir.isDirectory())
//            throw new IllegalArgumentException("Invalid directory " + directory);
//
//        SSTableLoader.Client client = new SSTableLoader.Client()
//        {
//            public void init(String keyspace)
//            {
//                for (Map.Entry<Range, List<InetAddress>> entry : StorageService.instance.getRangeToAddressMap(keyspace).entrySet())
//                {
//                    Range range = entry.getKey();
//                    for (InetAddress endpoint : entry.getValue())
//                        addRangeForEndpoint(range, endpoint);
//                }
//            }
//
//            public boolean validateColumnFamily(String keyspace, String cfName)
//            {
//                return Schema.instance.getCFMetaData(keyspace, cfName) != null;
//            }
//        };
//
//        SSTableLoader.OutputHandler oh = new SSTableLoader.OutputHandler()
//        {
//            public void output(String msg) { logger_.info(msg); }
//            public void debug(String msg) { logger_.debug(msg); }
//        };
//
//        SSTableLoader loader = new SSTableLoader(dir, client, oh);
//        try
//        {
//            loader.stream().get();
//        }
//        catch (Exception e)
//        {
//            throw new RuntimeException(e);
//        }
    }

    public int getExceptionCount()
    {
        return AbstractCassandraDaemon.exceptions.get();
    }

    public void rescheduleFailedDeletions()
    {
        SSTableDeletingTask.rescheduleFailedTasks();
    }

    /**
     * #{@inheritDoc}
     */
    public void loadNewSSTables(String ksName, String cfName)
    {
        ColumnFamilyStore.loadNewSSTables(ksName, cfName);
    }
}
