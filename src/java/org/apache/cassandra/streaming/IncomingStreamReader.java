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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncomingStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingStreamReader.class);

    protected final PendingFile localFile;
    protected final PendingFile remoteFile;
    protected final StreamInSession session;
    private final Socket socket;
    private final String basePath;
    
    private DataInputStream input;
    
    public IncomingStreamReader(StreamHeader header, Socket socket) throws IOException
    {
        socket.setSoTimeout(DatabaseDescriptor.getStreamingSocketTimeout());
        this.socket = socket;
        InetSocketAddress remoteAddress = (InetSocketAddress)socket.getRemoteSocketAddress();
        session = StreamInSession.get(remoteAddress.getAddress(), header.sessionId);
        session.addFiles(header.pendingFiles);
        // set the current file we are streaming so progress shows up in jmx
        session.setCurrentFile(header.file);
        // pendingFile gets the new context for the local node.
        remoteFile = header.file;
        localFile = remoteFile != null ? StreamIn.getContextMapping(remoteFile) : null;
        
        basePath = DatabaseDescriptor.getAllDataFileLocationsForTable(
        		remoteFile.desc.ksname)[0] + File.separator 
        		+ session.getHost().getHostAddress() + File.separator;
    }

    public void read() throws IOException
    {
        if (remoteFile != null)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Receiving stream");
                logger.debug("Creating files for {} with {} estimated keys",
                             localFile.desc.toString(),
                             remoteFile.estimatedKeys);
            }
            
            input = new DataInputStream(socket.getInputStream());
            
            if(!remoteFile.components.containsKey(Component.STATS.name()))
            {
            	logger.info("Error: " + remoteFile.desc.toString() + " not containing STATS");
            }
            
            try
            {
            	while(true)
            	{
            		int toRead = input.read();
                    byte[] receiveBuffer = new byte[toRead];
                    input.readFully(receiveBuffer);
                    
                    String str = new String(receiveBuffer);
                    if(str.equals(FileScpTask.SUCCESS))
                    {
                    	for(String component : remoteFile.components.keySet())
                    	{
                    		String remoteName = new File(remoteFile.getFilename(component)).getName();
                    		String localPath = localFile.getFilename(component);
                    		FileUtils.renameWithConfirm(
                    				new File(basePath + remoteName), new File(localPath));
                    	}
                    	break;
                    }
                    else if(str.equals(FileScpTask.FAIL))
                    {
                    	throw new IOException("Receive 'FAIL' in Incoming Stream");
                    }
                    else if(remoteFile.components.containsKey(str))
                    {
                    	String filename = localFile.desc.filenameFor(str);
                    	logger.info("Start to receive " + filename + " / " 
                    			+ remoteFile.components.get(str) + " bytes");
                    }
                    else
                    {
                    	// do nothing
                    }
            	}
            }
            catch(IOException ex)
            {
            	retry();
            	throw ex;
            }
            finally
            {
            	input.close();
            }
            
//            assert remoteFile.estimatedKeys > 0;
//            
//            input = new DataInputStream(new LZFInputStream(socket.getInputStream()));
//            List<String> componentList = FileStreamTask.getStreamingSortedListFor(remoteFile.components.keySet());
//            SequentialWriter file = null;
//            
//            try
//            {
//            	for(String component : componentList)
//                {
//                	String componentFileName = localFile.getFilename(component);
//                	logger.debug("Creating file for " + componentFileName);
//                	
//                	file = SequentialWriter.open(new File(componentFileName));
//                	long length = remoteFile.components.get(component);
//                	long bytesReceived = 0;
//
//                    while (bytesReceived < length)
//                    {
//                        long lastRead = streamIn(file, length, bytesReceived);
//                        bytesReceived += lastRead;
//                    }
//                    
//                    FileUtils.closeQuietly(file);
//                }
//            }
//            catch (IOException ex)
//            {
//                retry();
//                throw ex;
//            }
//            finally
//            {
//            	FileUtils.closeQuietly(file);
//                input.close();
//            }

            session.finished(remoteFile, localFile);
        }

        session.closeIfFinished();
    }

//    private int streamIn(SequentialWriter writer, long length, long bytesReceived) throws IOException
//    {
//        int toRead = (int) Math.min(FileStreamTask.CHUNK_SIZE, length - bytesReceived);
//
//        input.readFully(receiveBuffer, 0, toRead);
//        writer.write(receiveBuffer, 0, toRead);
//
//        return toRead;
//    }

    private void retry() throws IOException
    {
        /* Ask the source node to re-stream this file. */
        session.retry(remoteFile);

        /* Delete the orphaned file. */
        for(String component : remoteFile.components.keySet())
        {
        	String remoteName = new File(remoteFile.getFilename(component)).getName();
        	
        	FileUtils.deleteWithConfirm(new File(basePath + remoteName));
        }
    }
}
