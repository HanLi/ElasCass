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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class FileScpTask extends WrappedRunnable
{
    private static Logger logger = LoggerFactory.getLogger(FileScpTask.class);

    // around 10 minutes at the default rpctimeout
    public static final int MAX_CONNECT_ATTEMPTS = 8;

    public static final String scp_bash = "/usr/local/cassandra/bin/scp.sh";
    public static final String SUCCESS = "SUCCESS";
    public static final String FAIL = "FAIL";
    
    protected final StreamHeader header;
    protected final InetAddress to;

    // communication socket
    private Socket socket;
    // socket's output stream
    private OutputStream output;

    public FileScpTask(StreamHeader header, InetAddress to)
    {
        this.header = header;
        this.to = to;
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

        List<String> componentList = Lists.newArrayList(header.file.components.keySet());
        
        String[] cmd = new String[5];
    	cmd[0] = scp_bash;
    	cmd[1] = to.getHostAddress(); // target host
    	cmd[2] = DatabaseDescriptor.getAllDataFileLocationsForTable(header.file.desc.ksname)[0];
    	cmd[3] = FBUtilities.getBroadcastAddress().getHostAddress(); // folder name at destination
    	
    	for(String component : componentList)
        {
        	cmd[4] = header.file.getFilename(component);
        	logger.info("Transfering " + cmd[4] + " to " + cmd[1]);
        	write(component);
        	
    		ProcessBuilder pb = new ProcessBuilder(cmd);
    		Process process = pb.start();
    		try {
				process.waitFor();
			} 
    		catch (InterruptedException e) 
    		{
				e.printStackTrace();
				write(FAIL);
				return;
			}
        }
    	write(SUCCESS);
    }

    
    protected int write(String str) throws IOException
    {
    	// range 0 ~ 255 only
    	output.write(str.length());
        output.write(str.getBytes(), 0, str.length());
        output.flush();
        return str.length()+1;
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
        return String.format("FileScpTask(session=%s, to=%s)", header.sessionId, to);
    }
}
