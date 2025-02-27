/*
*
* Copyright 2013 Netflix, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/
package com.netflix.loadbalancer;

import com.google.common.annotations.VisibleForTesting;

import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfigKey;
import com.netflix.client.config.Property;
import com.netflix.client.config.UnboxedIntProperty;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.stats.distribution.DataDistribution;
import com.netflix.stats.distribution.DataPublisher;
import com.netflix.stats.distribution.Distribution;
import com.netflix.util.MeasuredRate;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Capture various stats per Server(node) in the LoadBalancer
 * @author stonse
 * 在负载均衡器中获取每个服务的统计信息
 */
public class ServerStats {

    /**
     * 默认拉取信息间隔时间，默认 1 分钟
     */
    private static final int DEFAULT_PUBLISH_INTERVAL =  60 * 1000; // = 1 minute
    /**
     * 默认缓存数量，即 1 分钟 1000 次请求
     */
    private static final int DEFAULT_BUFFER_SIZE = 60 * 1000; // = 1000 requests/sec for 1 minute

    /**
     * 连接失败阈值
     */
    private final UnboxedIntProperty connectionFailureThreshold;
    /**
     * 超时熔断因子
     */
    private final UnboxedIntProperty circuitTrippedTimeoutFactor;
    /**
     * 最大超时时间阈值
     */
    private final UnboxedIntProperty maxCircuitTrippedTimeout;
    /**
     * 活跃连接统计时间窗口
     */
    private final UnboxedIntProperty activeRequestsCountTimeout;

    /**
     * 百分比
     */
    private static final double[] PERCENTS = makePercentValues();

    /**
     * 计数器，时间窗口内
     */
    private DataDistribution dataDist = new DataDistribution(1, PERCENTS); // in case
    /**
     * 数据发布器，默认 1 分钟发布一次
     */
    private DataPublisher publisher = null;
    /**
     * 用于统计最大、最小、平均数等信息，一直累加
     */
    private final Distribution responseTimeDist = new Distribution();

    /**
     * 缓冲池储量
     */
    int bufferSize = DEFAULT_BUFFER_SIZE;
    /**
     * 发布间隔时间
     */
    int publishInterval = DEFAULT_PUBLISH_INTERVAL;

    /**
     * 失败次数统计时间窗口，默认 1000 ms
     */
    long failureCountSlidingWindowInterval = 1000;

    /**
     * 上一秒失败次数，时间间隔基于 failureCountSlidingWindowInterval
     */
    private MeasuredRate serverFailureCounts = new MeasuredRate(failureCountSlidingWindowInterval);
    /**
     * 一个窗口请求次数，时间间隔为 300 s
     */
    private MeasuredRate requestCountInWindow = new MeasuredRate(300000L);

    /**
     * 服务
     */
    Server server;
    /**
     * 总请求数量
     */
    AtomicLong totalRequests = new AtomicLong();

    /**
     * 连续（successive）请求异常数量（这个连续发生在 Retry 重试期间）
     *
     * 在重试期间，但凡有一次成功了，就会把此参数置为 0（失败的话此参数就一直加）
     * 说明：只有在异常类型是 callErrorHandler.isCircuitTrippingException(e) 的时候，才会算作失败，才会 + 1
     * 默认情况下只有 SocketException/SocketTimeoutException 这两种异常才算失败哦
     */
    @VisibleForTesting
    AtomicInteger successiveConnectionFailureCount = new AtomicInteger(0);

    /**
     * 活跃请求数量（正在请求的数量，它能反应该 Server 的负载、压力）
     * 但凡只要开始执行 Sever 了，就 + 1
     * 但凡只要请求完成了/出错了，就 - 1
     */
    @VisibleForTesting
    AtomicInteger activeRequestsCount = new AtomicInteger(0);

    /**
     * 打开连接数量
     */
    @VisibleForTesting
    AtomicInteger openConnectionsCount = new AtomicInteger(0);

    /**
     * 最后一次失败的时间戳
     */
    private volatile long lastConnectionFailedTimestamp;
    /**
     * activeRequestsCount 的值最后变化的时间戳
     */
    private volatile long lastActiveRequestsCountChangeTimestamp;
    /**
     * 断路器熔断总时长（连续失败 >= 3次，增加 20~30 秒。具体增加多少秒，参考计算逻辑）
     */
    private AtomicLong totalCircuitBreakerBlackOutPeriod = new AtomicLong(0);
    /**
     * 最后访问时间戳
     */
    private volatile long lastAccessedTimestamp;
    /**
     * 首次连接时间戳，只会记录首次请求进来时的时间
     */
    private volatile long firstConnectionTimestamp = 0;

    public ServerStats() {
        connectionFailureThreshold = new UnboxedIntProperty(Property.of(LoadBalancerStats.CONNECTION_FAILURE_COUNT_THRESHOLD.defaultValue()));
        circuitTrippedTimeoutFactor = new UnboxedIntProperty(LoadBalancerStats.CIRCUIT_TRIP_TIMEOUT_FACTOR_SECONDS.defaultValue());
        maxCircuitTrippedTimeout = new UnboxedIntProperty(LoadBalancerStats.CIRCUIT_TRIP_MAX_TIMEOUT_SECONDS.defaultValue());
        activeRequestsCountTimeout = new UnboxedIntProperty(LoadBalancerStats.ACTIVE_REQUESTS_COUNT_TIMEOUT.defaultValue());
    }

    /**
     * 创建 ServerStats
     * @param lbStats LoadBalancerStats
     */
    public ServerStats(LoadBalancerStats lbStats) {
        // 设置最大熔断超时时间
        maxCircuitTrippedTimeout = lbStats.getCircuitTripMaxTimeoutSeconds();
        // 设置超时因子
        circuitTrippedTimeoutFactor = lbStats.getCircuitTrippedTimeoutFactor();
        // 设置连接失败熔断阈值
        connectionFailureThreshold = lbStats.getConnectionFailureCountThreshold();
        // 设置活跃连接数时间窗口
        activeRequestsCountTimeout = lbStats.getActiveRequestsCountTimeout();
    }
    
    /**
     * Initializes the object, starting data collection and reporting.
     *
     * 初始化，开始数据收集和报告
     */
    public void initialize(Server server) {
        serverFailureCounts = new MeasuredRate(failureCountSlidingWindowInterval);
        requestCountInWindow = new MeasuredRate(300000L);
        if (publisher == null) {
            dataDist = new DataDistribution(getBufferSize(), PERCENTS);
            publisher = new DataPublisher(dataDist, getPublishIntervalMillis());
            // 开始收集
            publisher.start();
        }
        this.server = server;
    }
    
    public void close() {
        if (publisher != null)
            publisher.stop();
    }

    public Server getServer() {
        return server;
    }

    private int getBufferSize() {
        return bufferSize;
    }

    private long getPublishIntervalMillis() {
        return publishInterval;
    }
    
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setPublishInterval(int publishInterval) {
        this.publishInterval = publishInterval;
    }

    /**
     * The supported percentile values.
     * These correspond to the various Monitor methods defined below.
     * No, this is not pretty, but that's the way it is.
     *
     * 针对各种统计指标设置的百分比枚举值 【10， 25， 50， 75， 90， 95， 98， 99， 99.5】
     */
    private static enum Percent {

        TEN(10), TWENTY_FIVE(25), FIFTY(50), SEVENTY_FIVE(75), NINETY(90),
        NINETY_FIVE(95), NINETY_EIGHT(98), NINETY_NINE(99), NINETY_NINE_POINT_FIVE(99.5);

        private double val;

        Percent(double val) {
            this.val = val;
        }

        public double getValue() {
            return val;
        }

    }

    /**
     * 返回百分比枚举数组
     * @return
     */
    private static double[] makePercentValues() {
        Percent[] percents = Percent.values();
        double[] p = new double[percents.length];
        for (int i = 0; i < percents.length; i++) {
            p[i] = percents[i].getValue();
        }
        return p;
    }

    public long getFailureCountSlidingWindowInterval() {
        return failureCountSlidingWindowInterval;
    }

    public void setFailureCountSlidingWindowInterval(
            long failureCountSlidingWindowInterval) {
        this.failureCountSlidingWindowInterval = failureCountSlidingWindowInterval;
    }

    // run time methods
    
    
    /**
     * Increment the count of failures for this Server
     * 
     */
    public void addToFailureCount(){
        serverFailureCounts.increment();
    }
    
    /**
     * Returns the count of failures in the current window
     * 
     */
    public long getFailureCount(){
        return serverFailureCounts.getCurrentCount();
    }
    
    /**
     * Call this method to note the response time after every request
     * @param msecs
     *
     * 请求响应时间
     */
    public void noteResponseTime(double msecs){
        // 累加 dataDist
        dataDist.noteValue(msecs);
        // 累加 responseTimeDist
        responseTimeDist.noteValue(msecs);
    }
    
    public void incrementNumRequests(){
        totalRequests.incrementAndGet();
    }
    
    public void incrementActiveRequestsCount() {        
        activeRequestsCount.incrementAndGet();
        requestCountInWindow.increment();
        long currentTime = System.currentTimeMillis();
        lastActiveRequestsCountChangeTimestamp = currentTime;
        lastAccessedTimestamp = currentTime;
        if (firstConnectionTimestamp == 0) {
            firstConnectionTimestamp = currentTime;
        }
    }

    public void incrementOpenConnectionsCount() {
        openConnectionsCount.incrementAndGet();
    }

    public void decrementActiveRequestsCount() {
        activeRequestsCount.getAndUpdate(current -> Math.max(0, current - 1));
        lastActiveRequestsCountChangeTimestamp = System.currentTimeMillis();
    }

    public void decrementOpenConnectionsCount() {
        openConnectionsCount.getAndUpdate(current -> Math.max(0, current - 1));
    }

    public int  getActiveRequestsCount() {
        return getActiveRequestsCount(System.currentTimeMillis());
    }

    public int getActiveRequestsCount(long currentTime) {
        int count = activeRequestsCount.get();
        if (count == 0) {
            return 0;
        } else if (currentTime - lastActiveRequestsCountChangeTimestamp > activeRequestsCountTimeout.get() * 1000 || count < 0) {
            activeRequestsCount.set(0);
            return 0;            
        } else {
            return count;
        }
    }

    public int getOpenConnectionsCount() {
        return openConnectionsCount.get();
    }

    public long getMeasuredRequestsCount() {
        return requestCountInWindow.getCount();
    }

    @Monitor(name="ActiveRequestsCount", type = DataSourceType.GAUGE)    
    public int getMonitoredActiveRequestsCount() {
        return activeRequestsCount.get();
    }
    
    @Monitor(name="CircuitBreakerTripped", type = DataSourceType.INFORMATIONAL)    
    public boolean isCircuitBreakerTripped() {
        return isCircuitBreakerTripped(System.currentTimeMillis());
    }
    
    public boolean isCircuitBreakerTripped(long currentTime) {
        long circuitBreakerTimeout = getCircuitBreakerTimeout();
        if (circuitBreakerTimeout <= 0) {
            return false;
        }
        return circuitBreakerTimeout > currentTime;
    }

    private long getCircuitBreakerTimeout() {
        long blackOutPeriod = getCircuitBreakerBlackoutPeriod();
        if (blackOutPeriod <= 0) {
            return 0;
        }
        return lastConnectionFailedTimestamp + blackOutPeriod;
    }
    
    private long getCircuitBreakerBlackoutPeriod() {
        int failureCount = successiveConnectionFailureCount.get();
        int threshold = connectionFailureThreshold.get();
        if (failureCount < threshold) {
            return 0;
        }
        int diff = Math.min(failureCount - threshold, 16);
        int blackOutSeconds = (1 << diff) * circuitTrippedTimeoutFactor.get();
        if (blackOutSeconds > maxCircuitTrippedTimeout.get()) {
            blackOutSeconds = maxCircuitTrippedTimeout.get();
        }
        return blackOutSeconds * 1000L;
    }
    
    public void incrementSuccessiveConnectionFailureCount() {
        lastConnectionFailedTimestamp = System.currentTimeMillis();
        successiveConnectionFailureCount.incrementAndGet();
        totalCircuitBreakerBlackOutPeriod.addAndGet(getCircuitBreakerBlackoutPeriod());
    }
    
    public void clearSuccessiveConnectionFailureCount() {
        successiveConnectionFailureCount.set(0);
    }
    
    @Monitor(name="SuccessiveConnectionFailureCount", type = DataSourceType.GAUGE)
    public int getSuccessiveConnectionFailureCount() {
        return successiveConnectionFailureCount.get();
    }
    
    /*
     * Response total times
     */

    /**
     * Gets the average total amount of time to handle a request, in milliseconds.
     */
    @Monitor(name = "OverallResponseTimeMillisAvg", type = DataSourceType.INFORMATIONAL,
             description = "Average total time for a request, in milliseconds")
    public double getResponseTimeAvg() {
        return responseTimeDist.getMean();
    }

    /**
     * Gets the maximum amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "OverallResponseTimeMillisMax", type = DataSourceType.INFORMATIONAL,
             description = "Max total time for a request, in milliseconds")
    public double getResponseTimeMax() {
        return responseTimeDist.getMaximum();
    }

    /**
     * Gets the minimum amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "OverallResponseTimeMillisMin", type = DataSourceType.INFORMATIONAL,
             description = "Min total time for a request, in milliseconds")
    public double getResponseTimeMin() {
        return responseTimeDist.getMinimum();
    }

    /**
     * Gets the standard deviation in the total amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "OverallResponseTimeMillisStdDev", type = DataSourceType.INFORMATIONAL,
             description = "Standard Deviation in total time to handle a request, in milliseconds")
    public double getResponseTimeStdDev() {
        return responseTimeDist.getStdDev();
    }

    /*
     * QOS percentile performance data for most recent period
     */

    /**
     * Gets the number of samples used to compute the various response-time percentiles.
     */
    @Monitor(name = "ResponseTimePercentileNumValues", type = DataSourceType.GAUGE,
             description = "The number of data points used to compute the currently reported percentile values")
    public int getResponseTimePercentileNumValues() {
        return dataDist.getSampleSize();
    }

    /**
     * Gets the time when the varios percentile data was last updated.
     */
    @Monitor(name = "ResponseTimePercentileWhen", type = DataSourceType.INFORMATIONAL,
             description = "The time the percentile values were computed")
    public String getResponseTimePercentileTime() {
        return dataDist.getTimestamp();
    }

    /**
     * Gets the time when the varios percentile data was last updated,
     * in milliseconds since the epoch.
     */
    @Monitor(name = "ResponseTimePercentileWhenMillis", type = DataSourceType.COUNTER,
             description = "The time the percentile values were computed in milliseconds since the epoch")
    public long getResponseTimePercentileTimeMillis() {
        return dataDist.getTimestampMillis();
    }

    /**
     * Gets the average total amount of time to handle a request
     * in the recent time-slice, in milliseconds.
     */
    @Monitor(name = "ResponseTimeMillisAvg", type = DataSourceType.GAUGE,
             description = "Average total time for a request in the recent time slice, in milliseconds")
    public double getResponseTimeAvgRecent() {
        return dataDist.getMean();
    }
    
    /**
     * Gets the 10-th percentile in the total amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "ResponseTimeMillis10Percentile", type = DataSourceType.INFORMATIONAL,
             description = "10th percentile in total time to handle a request, in milliseconds")
    public double getResponseTime10thPercentile() {
        return getResponseTimePercentile(Percent.TEN);
    }

    /**
     * Gets the 25-th percentile in the total amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "ResponseTimeMillis25Percentile", type = DataSourceType.INFORMATIONAL,
             description = "25th percentile in total time to handle a request, in milliseconds")
    public double getResponseTime25thPercentile() {
        return getResponseTimePercentile(Percent.TWENTY_FIVE);
    }

    /**
     * Gets the 50-th percentile in the total amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "ResponseTimeMillis50Percentile", type = DataSourceType.INFORMATIONAL,
             description = "50th percentile in total time to handle a request, in milliseconds")
    public double getResponseTime50thPercentile() {
        return getResponseTimePercentile(Percent.FIFTY);
    }

    /**
     * Gets the 75-th percentile in the total amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "ResponseTimeMillis75Percentile", type = DataSourceType.INFORMATIONAL,
             description = "75th percentile in total time to handle a request, in milliseconds")
    public double getResponseTime75thPercentile() {
        return getResponseTimePercentile(Percent.SEVENTY_FIVE);
    }

    /**
     * Gets the 90-th percentile in the total amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "ResponseTimeMillis90Percentile", type = DataSourceType.INFORMATIONAL,
             description = "90th percentile in total time to handle a request, in milliseconds")
    public double getResponseTime90thPercentile() {
        return getResponseTimePercentile(Percent.NINETY);
    }

    /**
     * Gets the 95-th percentile in the total amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "ResponseTimeMillis95Percentile", type = DataSourceType.GAUGE,
             description = "95th percentile in total time to handle a request, in milliseconds")
    public double getResponseTime95thPercentile() {
        return getResponseTimePercentile(Percent.NINETY_FIVE);
    }

    /**
     * Gets the 98-th percentile in the total amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "ResponseTimeMillis98Percentile", type = DataSourceType.INFORMATIONAL,
             description = "98th percentile in total time to handle a request, in milliseconds")
    public double getResponseTime98thPercentile() {
        return getResponseTimePercentile(Percent.NINETY_EIGHT);
    }

    /**
     * Gets the 99-th percentile in the total amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "ResponseTimeMillis99Percentile", type = DataSourceType.GAUGE,
             description = "99th percentile in total time to handle a request, in milliseconds")
    public double getResponseTime99thPercentile() {
        return getResponseTimePercentile(Percent.NINETY_NINE);
    }

    /**
     * Gets the 99.5-th percentile in the total amount of time spent handling a request, in milliseconds.
     */
    @Monitor(name = "ResponseTimeMillis99_5Percentile", type = DataSourceType.GAUGE,
             description = "99.5th percentile in total time to handle a request, in milliseconds")
    public double getResponseTime99point5thPercentile() {
        return getResponseTimePercentile(Percent.NINETY_NINE_POINT_FIVE);
    }

    public long getTotalRequestsCount() {
        return totalRequests.get();
    }
    
    private double getResponseTimePercentile(Percent p) {
        return dataDist.getPercentiles()[p.ordinal()];
    }
    
    public String toString(){
        StringBuilder sb = new StringBuilder();
        
        sb.append("[Server:" + server + ";");
        sb.append("\tZone:" + server.getZone() + ";");
        sb.append("\tTotal Requests:" + totalRequests + ";");
        sb.append("\tSuccessive connection failure:" + getSuccessiveConnectionFailureCount() + ";");
        if (isCircuitBreakerTripped()) {
            sb.append("\tBlackout until: " + new Date(getCircuitBreakerTimeout()) + ";");
        }
        sb.append("\tTotal blackout seconds:" + totalCircuitBreakerBlackOutPeriod.get() / 1000 + ";");
        sb.append("\tLast connection made:" + new Date(lastAccessedTimestamp) + ";");
        if (lastConnectionFailedTimestamp > 0) {
            sb.append("\tLast connection failure: " + new Date(lastConnectionFailedTimestamp)  + ";");
        }
        sb.append("\tFirst connection made: " + new Date(firstConnectionTimestamp)  + ";");
        sb.append("\tActive Connections:" + getMonitoredActiveRequestsCount()  + ";");
        sb.append("\ttotal failure count in last (" + failureCountSlidingWindowInterval + ") msecs:" + getFailureCount()  + ";");
        sb.append("\taverage resp time:" + getResponseTimeAvg()  + ";");
        sb.append("\t90 percentile resp time:" + getResponseTime90thPercentile()  + ";");
        sb.append("\t95 percentile resp time:" + getResponseTime95thPercentile()  + ";");
        sb.append("\tmin resp time:" + getResponseTimeMin()  + ";");
        sb.append("\tmax resp time:" + getResponseTimeMax()  + ";");
        sb.append("\tstddev resp time:" + getResponseTimeStdDev());
        sb.append("]\n");
        
        return sb.toString();
    }
}
