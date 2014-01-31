package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

public class RangeMetadata {
	
	private static Logger logger = LoggerFactory.getLogger(RangeMetadata.class);
	
	private final InetAddress endpoint;

	/** a mapping from column family ID to a collection of ranges */
	private SetMultimap<Integer, Range> cfToRangeMap;
	
	public RangeMetadata(InetAddress ep)
	{
		this(ep, null);
	}
	
	public RangeMetadata(InetAddress ep, SetMultimap<Integer, Range> map)
	{
		endpoint = ep;
		if(map==null)
			map = Multimaps.synchronizedSetMultimap(HashMultimap.<Integer, Range>create());
		
		cfToRangeMap = map;
	}
	
	public void addRange(Integer cfId, Range range)
	{
		if(cfId != null)
			cfToRangeMap.put(cfId, range);
	}
	
	public void addRange(String ks, String cf, Range range)
	{
		addRange(Schema.instance.getId(ks, cf), range);
	}
	
	public void addRanges(Integer cfId, Iterable<Range> ranges)
	{
		if(cfId != null)
			cfToRangeMap.putAll(cfId, ranges);
	}
	
	public void addRanges(String ks, String cf, Iterable<Range> ranges)
	{
		addRanges(Schema.instance.getId(ks, cf), ranges);
	}
	
	public void removeRange(Integer cfId, Range range)
	{
		if(cfId != null)
			cfToRangeMap.remove(cfId, range);
	}
	
	public void removeRange(String ks, String cf, Range range)
	{
		removeRange(Schema.instance.getId(ks, cf), range);
	}
	
	public void removeAll(Integer cfId)
	{
		if(cfId != null)
			cfToRangeMap.removeAll(cfId);
	}
	
	public void removeAll(String ks, String cf)
	{
		removeAll(Schema.instance.getId(ks, cf));
	}
	
	public Set<Range> getRanges(Integer cfId)
	{
		if(cfId != null)
			return cfToRangeMap.get(cfId);
		
		return null;
	}
	
	public Set<Range> getRanges(String ks, String cf)
	{
		return getRanges(Schema.instance.getId(ks, cf));
	}
	
	public Map<Integer, Collection<Range>> getAllRanges()
	{
		return cfToRangeMap.asMap();
	}
	
	public boolean isEmpty()
	{
		return cfToRangeMap.values().isEmpty();
	}
	
	public InetAddress getEndpoint() {
		return endpoint;
	}
}
