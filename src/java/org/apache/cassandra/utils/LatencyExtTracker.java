package org.apache.cassandra.utils;

public class LatencyExtTracker extends LatencyTracker {
	
	private final long timeWindowInMillis;
	private long lastCallTimeInMillis;
	private long lastOpCount;
	private double lastOpCountPerSec;
	
	public LatencyExtTracker(int sec)
	{
		super();
		timeWindowInMillis = 1000 * sec;
		lastOpCount = 0;
		lastOpCountPerSec = 0.0;
		lastCallTimeInMillis = System.currentTimeMillis();
	}
	
	public double getOpCountPerSec()
    {
        long count = super.getOpCount();
        long recentCount = count - lastOpCount;
        
        long currentTime = System.currentTimeMillis();
        long elapsedTime = currentTime - lastCallTimeInMillis;
        
        double value = (1000.0 * recentCount + lastOpCountPerSec * timeWindowInMillis) / (elapsedTime + timeWindowInMillis);
        if(elapsedTime > timeWindowInMillis)
        {
        	lastOpCount = count;
        	lastOpCountPerSec = value;
        	lastCallTimeInMillis = currentTime;
        }
        return value;
    }

}
