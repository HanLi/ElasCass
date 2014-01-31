package org.apache.cassandra.streaming;
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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.net.MessagingService;

/**
 * Represents portions of a file to be streamed between nodes.
 */
public class PendingFile
{
    private static PendingFileSerializer serializer_ = new PendingFileSerializer();

    public static PendingFileSerializer serializer()
    {
        return serializer_;
    }

    // NB: this reference is used to be able to release the acquired reference upon completion
    public final SSTableReader sstable;

    public final Descriptor desc;
    public final Map<String, Long> components;
    public final OperationType type;
    public final long size;
    public final long estimatedKeys;
    public long progress;

    public PendingFile(Descriptor desc, PendingFile pf)
    {
        this(null, desc, pf.components, pf.type, pf.size, pf.estimatedKeys);
    }
    
    public PendingFile(SSTableReader sstable, Descriptor desc, Map<String, Long> components, 
    		OperationType type, long bytesOnDisk, long estimatedKeys)
    {
        this.sstable = sstable;
        this.desc = desc;
        this.components = new HashMap<String, Long>(components);
        this.type = type;
        this.size = bytesOnDisk;
        this.estimatedKeys = estimatedKeys;
    }

    public String getFilename(String component)
    {
        return desc.filenameFor(component);
    }
    
    public boolean equals(Object o)
    {
        if ( !(o instanceof PendingFile) )
            return false;

        PendingFile rhs = (PendingFile)o;
        return desc.equals(rhs.desc);
    }

    public int hashCode()
    {
        return desc.toString().hashCode();
    }

    public String toString()
    {
        return desc.toString() + " components=" + components.size() + " progress=" + progress + "/" + size + " - " + progress*100/size + "%";
    }

    public static class PendingFileSerializer implements IVersionedSerializer<PendingFile>
    {
        public void serialize(PendingFile sc, DataOutput dos, int version) throws IOException
        {
            if (sc == null)
            {
                dos.writeUTF("");
                return;
            }

            dos.writeUTF(sc.desc.filenameFor(Component.DATA));
            dos.writeInt(sc.components.size());
            for (Entry<String, Long> component : sc.components.entrySet())
            {
                dos.writeUTF(component.getKey());
                dos.writeLong(component.getValue());
            }
            
            dos.writeLong(sc.size);
            
            if (version > MessagingService.VERSION_07)
                dos.writeUTF(sc.type.name());
            if (version > MessagingService.VERSION_080)
                dos.writeLong(sc.estimatedKeys);
        }

        public PendingFile deserialize(DataInput dis, int version) throws IOException
        {
            String filename = dis.readUTF();
            if (filename.isEmpty())
                return null;
            
            Descriptor desc = Descriptor.fromFilename(filename);
            int count = dis.readInt();
            Map<String, Long> components = new HashMap<String, Long>();
            for (int i = 0; i < count; i++)
            {
            	String name = dis.readUTF();
            	Long length = dis.readLong();
            	components.put(name, length);
            }
            
            long fileSize = dis.readLong();
            
            // this controls the way indexes are rebuilt when streaming in.  
            OperationType type = OperationType.RESTORE_REPLICA_COUNT;
            if (version > MessagingService.VERSION_07)
                type = OperationType.valueOf(dis.readUTF());
            long estimatedKeys = 0;
            if (version > MessagingService.VERSION_080)
                estimatedKeys = dis.readLong();
            return new PendingFile(null, desc, components, type, fileSize, estimatedKeys);
        }

        public long serializedSize(PendingFile pendingFile, int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}
