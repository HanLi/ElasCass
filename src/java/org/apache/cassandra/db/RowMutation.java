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

package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class RowMutation implements IMutation, MessageProducer
{
    private static RowMutationSerializer serializer_ = new RowMutationSerializer();
    public static final String FORWARD_HEADER = "FORWARD";

    public static RowMutationSerializer serializer()
    {
        return serializer_;
    }

    private String table_;
    private ByteBuffer key_;
    private ColumnFamily columnFamily_;
//    // map of column family id to mutations for that column family.
//    protected Map<Integer, ColumnFamily> modifications_ = new HashMap<Integer, ColumnFamily>();

    private Map<Integer, byte[]> preserializedBuffers = new HashMap<Integer, byte[]>();

    public RowMutation(String table, ByteBuffer key)
    {
        table_ = table;
        key_ = key;
    }

    public RowMutation(String table, Row row)
    {
        table_ = table;
        key_ = row.key.key;
        add(row.cf);
    }

    protected RowMutation(String table, ByteBuffer key, ColumnFamily cf)
    {
        table_ = table;
        key_ = key;
        add(cf);
    }

    public String getTable()
    {
        return table_;
    }

    public Integer getColumnFamilyId()
    {
        return columnFamily_.id();
    }

    public ByteBuffer key()
    {
        return key_;
    }

    public ColumnFamily getColumnFamily()
    {
        return columnFamily_;
    }

    /**
     * Returns mutation representing a Hints to be sent to <code>address</code>
     * as soon as it becomes available.
     * The format is the following:
     *
     * HintsColumnFamily: {        // cf
     *   <dest token>: {           // key
     *     <uuid>: {               // super-column
     *       table: <table>        // columns
     *       key: <key>
     *       mutation: <mutation>
     *       version: <version>
     *     }
     *   }
     * }
     *
     */
    public static RowMutation hintFor(RowMutation mutation, ByteBuffer token) throws IOException
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, token);
        ByteBuffer hintId = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());

        // determine the TTL for the RowMutation
        // this is set at the smallest GCGraceSeconds for any of the CFs in the RM
        // this ensures that deletes aren't "undone" by delivery of an old hint
        int ttl = mutation.getColumnFamily().metadata().getGcGraceSeconds();

        // serialized RowMutation
        QueryPath path = new QueryPath(HintedHandOffManager.HINTS_CF, hintId, ByteBufferUtil.bytes("mutation"));
        rm.add(path, ByteBuffer.wrap(mutation.getSerializedBuffer(MessagingService.version_)), System.currentTimeMillis(), ttl);

        // serialization version
        path = new QueryPath(HintedHandOffManager.HINTS_CF, hintId, ByteBufferUtil.bytes("version"));
        rm.add(path, ByteBufferUtil.bytes(MessagingService.version_), System.currentTimeMillis(), ttl);

        // table
        path = new QueryPath(HintedHandOffManager.HINTS_CF, hintId, ByteBufferUtil.bytes("table"));
        rm.add(path, ByteBufferUtil.bytes(mutation.getTable()), System.currentTimeMillis(), ttl);

        // key
        path = new QueryPath(HintedHandOffManager.HINTS_CF, hintId, ByteBufferUtil.bytes("key"));
        rm.add(path, mutation.key(), System.currentTimeMillis(), ttl);

        return rm;
    }

    /*
     * Specify a column family name and the corresponding column
     * family object.
     * param @ cf - column family name
     * param @ columnFamily - the column family.
     */
    public void add(ColumnFamily columnFamily)
    {
        assert columnFamily != null;
        if(columnFamily_ != null && columnFamily_.id().equals(columnFamily.id())==false)
        {
        	// developer error
            throw new IllegalArgumentException("ColumnFamily " + columnFamily 
            		+ " already has modifications in this mutation: " + columnFamily_);
        }
        columnFamily_ = columnFamily;
    }

    public boolean isEmpty()
    {
        return columnFamily_ == null;
    }

    /*
     * Specify a column name and a corresponding value for
     * the column. Column name is specified as <column family>:column.
     * This will result in a ColumnFamily associated with
     * <column family> as name and a Column with <column>
     * as name. The column can be further broken up
     * as super column name : columnname  in case of super columns
     *
     * param @ cf - column name as <column family>:<column>
     * param @ value - value associated with the column
     * param @ timestamp - timestamp associated with this data.
     * param @ timeToLive - ttl for the column, 0 for standard (non expiring) columns
     */
    public void add(QueryPath path, ByteBuffer value, long timestamp, int timeToLive)
    {
    	createColumnFamily(path);
    	columnFamily_.addColumn(path, value, timestamp, timeToLive);
    }

    public void addCounter(QueryPath path, long value)
    {
    	createColumnFamily(path);
        columnFamily_.addCounter(path, value);
    }

    public void add(QueryPath path, ByteBuffer value, long timestamp)
    {
        add(path, value, timestamp, 0);
    }

    public void delete(QueryPath path, long timestamp)
    {
    	createColumnFamily(path);
        int localDeleteTime = (int) (System.currentTimeMillis() / 1000);

        if (path.superColumnName == null && path.columnName == null)
        {
            columnFamily_.delete(localDeleteTime, timestamp);
        }
        else if (path.columnName == null)
        {
            SuperColumn sc = new SuperColumn(path.superColumnName, columnFamily_.getSubComparator());
            sc.delete(localDeleteTime, timestamp);
            columnFamily_.addColumn(sc);
        }
        else
        {
            columnFamily_.addTombstone(path, localDeleteTime, timestamp);
        }
    }
    
    private void createColumnFamily(QueryPath path)
    {
    	if(columnFamily_ == null)
        {
        	columnFamily_ = ColumnFamily.create(table_, path.columnFamilyName);
        }
    	
        Integer id = Schema.instance.getId(table_, path.columnFamilyName);
        if(id.equals(columnFamily_.id())==false)
        {
        	new UnsupportedOperationException("Cannot support multi-CF mutation anymore. Abort.");
        }
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
     */
    public void apply() throws IOException
    {
        KSMetaData ksm = Schema.instance.getTableDefinition(getTable());

        Table.open(table_).apply(this, ksm.durableWrites);
    }

    public void applyUnsafe() throws IOException
    {
        Table.open(table_).apply(this, false);
    }

    public Message getMessage(Integer version) throws IOException
    {
        return getMessage(StorageService.Verb.MUTATION, version);
    }

    public Message getMessage(StorageService.Verb verb, int version) throws IOException
    {
        return new Message(FBUtilities.getBroadcastAddress(), verb, getSerializedBuffer(version), version);
    }

    public synchronized byte[] getSerializedBuffer(int version) throws IOException
    {
        byte[] bytes = preserializedBuffers.get(version);
        if (bytes == null)
        {
            bytes = FBUtilities.serialize(this, serializer(), version);
            preserializedBuffers.put(version, bytes);
        }
        return bytes;
    }

    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean shallow)
    {
        StringBuilder buff = new StringBuilder("RowMutation(");
        buff.append("keyspace='").append(table_).append('\'');
        buff.append(", key='").append(ByteBufferUtil.bytesToHex(key_)).append('\'');
        buff.append(", modifications=[");
        if (shallow)
        {
        	CFMetaData cfm = Schema.instance.getCFMetaData(columnFamily_.id());
            String cfname = cfm == null ? "-dropped-" : cfm.cfName;
            buff.append(cfname);
        }
        else
            buff.append(columnFamily_);
        return buff.append("])").toString();
    }

    public void addColumnOrSuperColumn(String cfName, ColumnOrSuperColumn cosc)
    {
        if (cosc.super_column != null)
        {
            for (org.apache.cassandra.thrift.Column column : cosc.super_column.columns)
            {
                add(new QueryPath(cfName, cosc.super_column.name, column.name), column.value, column.timestamp, column.ttl);
            }
        }
        else if (cosc.column != null)
        {
            add(new QueryPath(cfName, null, cosc.column.name), cosc.column.value, cosc.column.timestamp, cosc.column.ttl);
        }
        else if (cosc.counter_super_column != null)
        {
            for (org.apache.cassandra.thrift.CounterColumn column : cosc.counter_super_column.columns)
            {
                addCounter(new QueryPath(cfName, cosc.counter_super_column.name, column.name), column.value);
            }
        }
        else // cosc.counter_column != null
        {
            addCounter(new QueryPath(cfName, null, cosc.counter_column.name), cosc.counter_column.value);
        }
    }

    public void deleteColumnOrSuperColumn(String cfName, Deletion del)
    {
        if (del.predicate != null && del.predicate.column_names != null)
        {
            for(ByteBuffer c : del.predicate.column_names)
            {
                if (del.super_column == null && Schema.instance.getColumnFamilyType(table_, cfName) == ColumnFamilyType.Super)
                    delete(new QueryPath(cfName, c), del.timestamp);
                else
                    delete(new QueryPath(cfName, del.super_column, c), del.timestamp);
            }
        }
        else
        {
            delete(new QueryPath(cfName, del.super_column), del.timestamp);
        }
    }

    public static RowMutation fromBytes(byte[] raw, int version) throws IOException
    {
        RowMutation rm = serializer_.deserialize(new DataInputStream(new FastByteArrayInputStream(raw)), version);
        if(rm.getColumnFamily().metadata().getDefaultValidator().isCommutative()==false)
        	rm.preserializedBuffers.put(version, raw);
        
        return rm;
    }

    public static class RowMutationSerializer implements IVersionedSerializer<RowMutation>
    {
        public void serialize(RowMutation rm, DataOutput dos, int version) throws IOException
        {
            dos.writeUTF(rm.getTable());
            ByteBufferUtil.writeWithShortLength(rm.key(), dos);

            /* serialize the columnFamily_ in the mutation */
            ColumnFamily.serializer().serialize(rm.getColumnFamily(), dos);
        }

        public RowMutation deserialize(DataInput dis, int version, IColumnSerializer.Flag flag) throws IOException
        {
            String table = dis.readUTF();
            ByteBuffer key = ByteBufferUtil.readWithShortLength(dis);
            ColumnFamily cf = ColumnFamily.serializer().deserialize(dis, flag, ThreadSafeSortedColumns.factory());
            return new RowMutation(table, key, cf);
        }

        public RowMutation deserialize(DataInput dis, int version) throws IOException
        {
            return deserialize(dis, version, IColumnSerializer.Flag.FROM_REMOTE);
        }

        public long serializedSize(RowMutation rm, int version)
        {
            int size = DBConstants.shortSize + FBUtilities.encodedUTF8Length(rm.getTable());
            size += DBConstants.shortSize + rm.key().remaining();
            size += rm.getColumnFamily().serializedSize();
            return size;
        }
    }
}
