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

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleReplicationStrategy extends AbstractReplicationStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleReplicationStrategy.class);

    public SimpleReplicationStrategy(String table, String cfName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) throws ConfigurationException
    {
        super(table, cfName, tokenMetadata, snitch, configOptions);
    }

    /**
     * Simply returns all the endpoints that have the token
     * The token should be presented in the ring already.
     */
    public List<InetAddress> calculateNaturalEndpoints(Token canonicalToken, TokenMetadata tokenMetadata)
    {
        return tokenMetadata.getEndpointsForReading(canonicalToken);
    }

    public int getReplicationFactor()
    {
    	return Integer.parseInt(this.configOptions.get("replication_factor"));
    }

    public void validateOptions() throws ConfigurationException
    {
    	if (configOptions == null || configOptions.get("replication_factor") == null)
        {
            throw new ConfigurationException(
            		"SimpleReplicationStrategy requires a replication_factor strategy option.");
        }
        validateReplicationFactor(configOptions.get("replication_factor"));
    }
}
