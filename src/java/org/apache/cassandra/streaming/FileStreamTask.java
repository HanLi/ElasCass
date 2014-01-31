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

package org.apache.cassandra.streaming;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throttle;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.ning.compress.lzf.LZFOutputStream;

public class FileStreamTask extends WrappedRunnable
{
    private static Logger logger = LoggerFactory.getLogger(FileStreamTask.class);
    
    public static final int CHUNK_SIZE = 64 * 1024;
    // around 10 minutes at the default rpctimeout
    public static final int MAX_CONNECT_ATTEMPTS = 8;

    protected final StreamHeader header;
    protected final InetAddress to;

    // communication socket
    private Socket socket;
    // socket's output stream
    private OutputStream output;
    // allocate buffer to use for transfers only once
    private final byte[] transferBuffer = new byte[CHUNK_SIZE];
    // outbound global throughput limiter
    private final Throttle throttle;

    public FileStreamTask(StreamHeader header, InetAddress to)
    {
        this.header = header;
        this.to = to;
        this.throttle = new Throttle(toString(), new Throttle.ThroughputFunction()
        {
            /** @return Instantaneous throughput target in bytes per millisecond. */
            public int targetThroughput()
            {
                if (DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec() < 1)
                    // throttling disabled
                    return 0;
                // total throughput
                int totalBytesPerMS = DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec() * 1024 * 1024 / 8 / 1000;
                // per stream throughput (target bytes per MS)
                return totalBytesPerMS / Math.max(1, MessagingService.instance().getActiveStreamsOutbound());
//                return totalBytesPerMS;
            }
        });
    }
    
    public void runMayThrow() throws IOException
    {
        try
        {
            connectAttempt();
            // successfully connected: stream.
            // (at this point, if we fail, it is the receiver's job to re-request)
            stream();
        }
        finally
        {
            try
            {
                close();
            }
            catch (IOException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("error closing socket", e);
            }
        }
        if (logger.isDebugEnabled())
            logger.debug("Done streaming " + header.file);
    }

    /**
     * Stream file by it's sections specified by this.header
     * @throws IOException on any I/O error
     */
    private void stream() throws IOException
    {
        ByteBuffer HeaderBuffer = MessagingService.instance().constructStreamHeader(header, false, Gossiper.instance.getVersion(to));
        // write header (this should not be compressed for compatibility with other messages)
        output.write(ByteBufferUtil.getArray(HeaderBuffer));

        if (header.file == null)
            return;

        // setting up data compression stream
        output = new LZFOutputStream(output);
        
        List<String> componentList = getStreamingSortedListFor(header.file.components.keySet());
        RandomAccessReader file = null;
        
        try
        {
        	for(String component : componentList)
            {
            	String fileName = header.file.getFilename(component);
            	file = RandomAccessReader.open(new File(fileName));
            	
            	long length = file.length();
            	long bytesTransferred = 0;

                while (bytesTransferred < length)
                {
                    long lastWrite = write(file, length, bytesTransferred);
                    bytesTransferred += lastWrite;
                    // store streaming progress
                    header.file.progress += lastWrite;
                }
                
                // make sure the current component is sent
                output.flush();
                if (logger.isDebugEnabled())
                    logger.debug("Bytes transferred " + bytesTransferred + "/" + header.file.size);
                
                FileUtils.closeQuietly(file);
            }
        }
        finally
        {
            // no matter what happens close file
            FileUtils.closeQuietly(file);
        }
    }

    /**
     * Sequentially read bytes from the file and write them to the output stream
     *
     * @param reader The file reader to read from
     * @param length The full length that should be transferred
     * @param bytesTransferred Number of bytes remaining to transfer
     *
     * @return Number of bytes transferred
     *
     * @throws IOException on any I/O error
     */
    protected long write(RandomAccessReader reader, long length, long bytesTransferred) throws IOException
    {
        int toTransfer = (int) Math.min(CHUNK_SIZE, length - bytesTransferred);

        reader.readFully(transferBuffer, 0, toTransfer);
        output.write(transferBuffer, 0, toTransfer);
//        throttle.throttleDelta(toTransfer);
        output.flush();
        return toTransfer;
    }

    /**
     * Connects to the destination, with backoff for failed attempts.
     * TODO: all nodes on a cluster must currently use the same storage port
     * @throws IOException If all attempts fail.
     */
    private void connectAttempt() throws IOException
    {
        int attempts = 0;
        while (true)
        {
            try
            {
                socket = MessagingService.instance().getConnectionPool(to).newSocket();
                socket.setSoTimeout(DatabaseDescriptor.getStreamingSocketTimeout());
                output = socket.getOutputStream();
                break;
            }
            catch (IOException e)
            {
                if (++attempts >= MAX_CONNECT_ATTEMPTS)
                    throw e;

                long waitms = DatabaseDescriptor.getRpcTimeout() * (long)Math.pow(2, attempts);
                logger.warn("Failed attempt " + attempts + " to connect to " + to + " to stream " + header.file + ". Retrying in " + waitms + " ms. (" + e + ")");
                try
                {
                    Thread.sleep(waitms);
                }
                catch (InterruptedException wtf)
                {
                    throw new RuntimeException(wtf);
                }
            }
        }
    }

    protected void close() throws IOException
    {
        output.close();
    }

    public String toString()
    {
        return String.format("FileStreamTask(session=%s, to=%s)", header.sessionId, to);
    }
    
    public static List<String> getStreamingSortedListFor(Set<String> components)
    {
    	ArrayList<String> list = Lists.newArrayList(components);
    	Collections.sort(list);
    	return list;
    }
}
