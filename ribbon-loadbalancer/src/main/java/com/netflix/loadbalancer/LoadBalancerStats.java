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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.netflix.client.IClientConfigAware;
import com.netflix.client.config.ClientConfigFactory;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.client.config.IClientConfigKey;
import com.netflix.client.config.UnboxedIntProperty;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Class that acts as a repository of operational charateristics and statistics
 * of every Node/Server in the LaodBalancer.
 * 
 * This information can be used to just observe and understand the runtime
 * behavior of the loadbalancer or more importantly for the basis that
 * determines the loadbalacing strategy
 * 
 * @author stonse
 *
 * 用于存储负载均衡器运行时的状态及统计信息
 *
 * 信息如下：
 * DynamicServerListLoadBalancer for client goods-service initialized:
 * DynamicServerListLoadBalancer:{NFLoadBalancer:name=goods-service,current list of
 * Servers=[localhost:9091, localhost:9081],Load balancer stats=Zone stats: {unknown=
 * [Zone:unknown; Instance count:2; Active connections count: 0; Circuit breaker
 * tripped count: 0; Active connections per server: 0.0;]
 * },Server stats: [[Server:localhost:9091; Zone:UNKNOWN; Total Requests:0;
 * Successive connection failure:0; Total blackout seconds:0; Last connection
 * made:Thu Jan 01 08:00:00 CST 1970; First connection made: Thu Jan 01 08:00:00 CST
 * 1970; Active Connections:0; total failure count in last (1000) msecs:0; average
 * resp time:0.0; 90 percentile resp time:0.0; 95 percentile resp time:0.0; min
 * resp time:0.0; max resp time:0.0; stddev resp time:0.0]
 * , [Server:localhost:9081; Zone:UNKNOWN; Total Requests:0; Successive connection
 * failure:0; Total blackout seconds:0; Last connection made:Thu Jan 01 08:00:00 CST
 * 1970; First connection made: Thu Jan 01 08:00:00 CST 1970; Active Connections:0;
 * total failure count in last (1000) msecs:0; average resp time:0.0; 90 percentile resp
 * time:0.0; 95 percentile resp time:0.0; min resp time:0.0; max resp time:0.0;
 * stddev resp time:0.0]
 * ]}ServerList:com.netflix.loadbalancer.ConfigurationBasedServerList@74ddb59a
 */
public class LoadBalancerStats implements IClientConfigAware {

    /**
     * 前缀
     */
    private static final String PREFIX = "LBStats_";

    /**
     * 统计活跃连接数量的时间窗口，默认 600s
     */
    public static final IClientConfigKey<Integer> ACTIVE_REQUESTS_COUNT_TIMEOUT = new CommonClientConfigKey<Integer>(
            "niws.loadbalancer.serverStats.activeRequestsCount.effectiveWindowSeconds", 60 * 10) {};

    /**
     * 连接失败阈值。
     * 默认 3， 超过就熔断
     *
     * 自定义
     */
    public static final IClientConfigKey<Integer> CONNECTION_FAILURE_COUNT_THRESHOLD = new CommonClientConfigKey<Integer>(
            "niws.loadbalancer.%s.connectionFailureCountThreshold", 3) {};

    /**
     * 断路器超时因子
     * 默认 10s
     *
     * 自定义
     */
    public static final IClientConfigKey<Integer> CIRCUIT_TRIP_TIMEOUT_FACTOR_SECONDS = new CommonClientConfigKey<Integer>(
            "niws.loadbalancer.%s.circuitTripTimeoutFactorSeconds", 10) {};

    /**
     * 断路器最大超时秒数。
     * 默认 30s = 10s(超时因子) * 3(连接失败阈值);
     *
     * 自定义
     */
    public static final IClientConfigKey<Integer> CIRCUIT_TRIP_MAX_TIMEOUT_SECONDS = new CommonClientConfigKey<Integer>(
            "niws.loadbalancer.%s.circuitTripMaxTimeoutSeconds", 30) {};

    /**
     * 连接失败阈值。
     * 默认 3， 超过就熔断
     *
     * 默认 default 配置
     */
    public static final IClientConfigKey<Integer> DEFAULT_CONNECTION_FAILURE_COUNT_THRESHOLD = new CommonClientConfigKey<Integer>(
            "niws.loadbalancer.default.connectionFailureCountThreshold", 3) {};

    /**
     * 断路器超时因子
     * 默认 10s
     *
     * 默认 default 配置
     */
    public static final IClientConfigKey<Integer> DEFAULT_CIRCUIT_TRIP_TIMEOUT_FACTOR_SECONDS = new CommonClientConfigKey<Integer>(
            "niws.loadbalancer.default.circuitTripTimeoutFactorSeconds", 10) {};

    /**
     * 断路器最大超时秒数。
     * 默认 30s = 10s(超时因子) * 3(连接失败阈值);
     *
     * 默认 default 配置
     */
    public static final IClientConfigKey<Integer> DEFAULT_CIRCUIT_TRIP_MAX_TIMEOUT_SECONDS = new CommonClientConfigKey<Integer>(
            "niws.loadbalancer.default.circuitTripMaxTimeoutSeconds", 30) {};

    /**
     * 名称
     */
    private String name;

    /**
     * zone 状态缓存
     * key: zone
     * value: ZoneStats
     */
    volatile Map<String, ZoneStats> zoneStatsMap = new ConcurrentHashMap<>();

    /**
     * 活跃服务列表
     * key: zone
     * value: 服务列表
     */
    volatile Map<String, List<? extends Server>> upServerListZoneMap = new ConcurrentHashMap<>();

    /**
     * 连接失败阈值
     */
    private UnboxedIntProperty connectionFailureThreshold = new UnboxedIntProperty(CONNECTION_FAILURE_COUNT_THRESHOLD.defaultValue());
    /**
     * 超时因子
     */
    private UnboxedIntProperty circuitTrippedTimeoutFactor = new UnboxedIntProperty(CIRCUIT_TRIP_TIMEOUT_FACTOR_SECONDS.defaultValue());
    /**
     * 最大熔断时间
     */
    private UnboxedIntProperty maxCircuitTrippedTimeout = new UnboxedIntProperty(CIRCUIT_TRIP_MAX_TIMEOUT_SECONDS.defaultValue());
    /**
     * 统计活跃连接数的时间窗口
     */
    private UnboxedIntProperty activeRequestsCountTimeout = new UnboxedIntProperty(ACTIVE_REQUESTS_COUNT_TIMEOUT.defaultValue());

    /**
     * 服务状态信息缓存
     * key: server
     * value: ServerStats
     *
     * 缓存失效时间: 30分钟
     */
    private final LoadingCache<Server, ServerStats> serverStatsCache = CacheBuilder.newBuilder()
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .removalListener((RemovalListener<Server, ServerStats>) notification -> notification.getValue().close())
            .build(new CacheLoader<Server, ServerStats>() {
                public ServerStats load(Server server) {
                    return createServerStats(server);
                }
            });

    /**
     * 创建服务状态信息
     * @param server 服务
     * @return ServerStats
     */
    protected ServerStats createServerStats(Server server) {
        // 创建 ServerStats
        ServerStats ss = new ServerStats(this);
        //configure custom settings
        // 设置缓存数量
        ss.setBufferSize(1000);
        // 设置刷新时间间隔
        ss.setPublishInterval(1000);
        // 初始化 ServerStats
        ss.initialize(server);
        return ss;        
    }

    public LoadBalancerStats() {

    }

    public LoadBalancerStats(String name) {
        this.name = name;

        Monitors.registerObject(name, this);
    }

    @Override
    public void initWithNiwsConfig(IClientConfig clientConfig) {
        this.name = clientConfig.getClientName();
        Preconditions.checkArgument(name != null, "IClientConfig#getCLientName() must not be null");
        this.connectionFailureThreshold = new UnboxedIntProperty(
                clientConfig.getGlobalProperty(CONNECTION_FAILURE_COUNT_THRESHOLD.format(name))
                    .fallbackWith(clientConfig.getGlobalProperty(DEFAULT_CONNECTION_FAILURE_COUNT_THRESHOLD))
        );
        this.circuitTrippedTimeoutFactor = new UnboxedIntProperty(
                clientConfig.getGlobalProperty(CIRCUIT_TRIP_TIMEOUT_FACTOR_SECONDS.format(name))
                        .fallbackWith(clientConfig.getGlobalProperty(DEFAULT_CIRCUIT_TRIP_TIMEOUT_FACTOR_SECONDS))
        );
        this.maxCircuitTrippedTimeout = new UnboxedIntProperty(
                clientConfig.getGlobalProperty(CIRCUIT_TRIP_MAX_TIMEOUT_SECONDS.format(name))
                        .fallbackWith(clientConfig.getGlobalProperty(DEFAULT_CIRCUIT_TRIP_MAX_TIMEOUT_SECONDS))
        );
        this.activeRequestsCountTimeout = new UnboxedIntProperty(
                clientConfig.getGlobalProperty(ACTIVE_REQUESTS_COUNT_TIMEOUT));
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    UnboxedIntProperty getConnectionFailureCountThreshold() {
        return connectionFailureThreshold;

    }

    UnboxedIntProperty getCircuitTrippedTimeoutFactor() {
        return circuitTrippedTimeoutFactor;
    }

    UnboxedIntProperty getCircuitTripMaxTimeoutSeconds() {
        return maxCircuitTrippedTimeout;
    }

    UnboxedIntProperty getActiveRequestsCountTimeout() {
        return activeRequestsCountTimeout;
    }

    /**
     * The caller o this class is tasked to call this method every so often if
     * the servers participating in the LoadBalancer changes
     * @param servers
     */
    public void updateServerList(List<Server> servers){
        for (Server s: servers){
            addServer(s);
        }
    }
    
    
    public void addServer(Server server) {
        if (server != null) {
            try {
                serverStatsCache.get(server);
            } catch (ExecutionException e) {
                ServerStats stats = createServerStats(server);
                serverStatsCache.asMap().putIfAbsent(server, stats);
            }
        }
    } 
    
    /**
     * Method that updates the internal stats of Response times maintained on a per Server
     * basis
     * @param server
     * @param msecs
     */
    public void noteResponseTime(Server server, double msecs){
        ServerStats ss = getServerStats(server);  
        ss.noteResponseTime(msecs);
    }

    /**
     * 获取服务状态信息
     * @param server 服务
     * @return 服务状态信息
     */
    protected ServerStats getServerStats(Server server) {
        // 服务为空，直接返回 null
        if (server == null) {
            return null;
        }
        try {
            return serverStatsCache.get(server);
        } catch (ExecutionException e) {
            ServerStats stats = createServerStats(server);
            serverStatsCache.asMap().putIfAbsent(server, stats);
            return serverStatsCache.asMap().get(server);
        }
    }
    
    public void incrementActiveRequestsCount(Server server) {
        ServerStats ss = getServerStats(server); 
        ss.incrementActiveRequestsCount();
    }

    public void decrementActiveRequestsCount(Server server) {
        ServerStats ss = getServerStats(server); 
        ss.decrementActiveRequestsCount();
    }

    private ZoneStats getZoneStats(String zone) {
        zone = zone.toLowerCase();
        ZoneStats zs = zoneStatsMap.get(zone);
        if (zs == null){
            zoneStatsMap.put(zone, new ZoneStats(this.getName(), zone, this));
            zs = zoneStatsMap.get(zone);
        }
        return zs;
    }

    
    public boolean isCircuitBreakerTripped(Server server) {
        ServerStats ss = getServerStats(server);
        return ss.isCircuitBreakerTripped();
    }
        
    public void incrementSuccessiveConnectionFailureCount(Server server) {
        ServerStats ss = getServerStats(server);
        ss.incrementSuccessiveConnectionFailureCount();
    }
    
    public void clearSuccessiveConnectionFailureCount(Server server) {
        ServerStats ss = getServerStats(server);
        ss.clearSuccessiveConnectionFailureCount();        
    }

    public void incrementNumRequests(Server server){
        ServerStats ss = getServerStats(server);  
        ss.incrementNumRequests();
    }

    public void incrementZoneCounter(Server server) {
        String zone = server.getZone();
        if (zone != null) {
            getZoneStats(zone).incrementCounter();
        }
    }
    
    public void updateZoneServerMapping(Map<String, List<Server>> map) {
        upServerListZoneMap = new ConcurrentHashMap<String, List<? extends Server>>(map);
        // make sure ZoneStats object exist for available zones for monitoring purpose
        for (String zone: map.keySet()) {
            getZoneStats(zone);
        }
    }

    public int getInstanceCount(String zone) {
        if (zone == null) {
            return 0;
        }
        zone = zone.toLowerCase();
        List<? extends Server> currentList = upServerListZoneMap.get(zone);
        if (currentList == null) {
            return 0;
        }
        return currentList.size();
    }
    
    public int getActiveRequestsCount(String zone) {
        return getZoneSnapshot(zone).getActiveRequestsCount();
    }
        
    public double getActiveRequestsPerServer(String zone) {
        return getZoneSnapshot(zone).getLoadPerServer();
    }

    /**
     * 获取 zone 对应的快照
     * @param zone zone
     * @return ZoneSnapshot
     */
    public ZoneSnapshot getZoneSnapshot(String zone) {
        // 如果 zone 为空，直接创建新的快照
        if (zone == null) {
            return new ZoneSnapshot();
        }
        // 小写
        zone = zone.toLowerCase();
        // 获取 zone 对应的活跃服务列表
        List<? extends Server> currentList = upServerListZoneMap.get(zone);
        // 获取 zone 对应的快照
        return getZoneSnapshot(currentList);        
    }
    
    /**
     * This is the core function to get zone stats. All stats are reported to avoid
     * going over the list again for a different stat.
     * 
     * @param servers 活跃服务列表
     *
     * 获取 zone 对应的快照
     */
    public ZoneSnapshot getZoneSnapshot(List<? extends Server> servers) {
        // 如果活跃服务列表不存在，创建新的快照信息
        if (servers == null || servers.size() == 0) {
            return new ZoneSnapshot();
        }
        // 服务数量
        int instanceCount = servers.size();
        // 活跃连接数量: 进入 server + 1, 失败或者完成 - 1
        int activeConnectionsCount = 0;
        int activeConnectionsCountOnAvailableServer = 0;
        int circuitBreakerTrippedCount = 0;
        double loadPerServer = 0;
        long currentTime = System.currentTimeMillis();
        // 遍历服务列表
        for (Server server: servers) {
            // 获取单个服务状态信息
            ServerStats stat = getSingleServerStat(server);   
            if (stat.isCircuitBreakerTripped(currentTime)) {
                circuitBreakerTrippedCount++;
            } else {
                activeConnectionsCountOnAvailableServer += stat.getActiveRequestsCount(currentTime);
            }
            activeConnectionsCount += stat.getActiveRequestsCount(currentTime);
        }
        if (circuitBreakerTrippedCount == instanceCount) {
            if (instanceCount > 0) {
                // should be NaN, but may not be displayable on Epic
                loadPerServer = -1;
            }
        } else {
            loadPerServer = ((double) activeConnectionsCountOnAvailableServer) / (instanceCount - circuitBreakerTrippedCount);
        }
        // 创建 ZoneSnapshot
        return new ZoneSnapshot(instanceCount, circuitBreakerTrippedCount, activeConnectionsCount, loadPerServer);
    }
    
    public int getCircuitBreakerTrippedCount(String zone) {
        return getZoneSnapshot(zone).getCircuitTrippedCount();
    }

    @Monitor(name=PREFIX + "CircuitBreakerTrippedCount", type = DataSourceType.GAUGE)   
    public int getCircuitBreakerTrippedCount() {
        int count = 0;
        for (String zone: upServerListZoneMap.keySet()) {
            count += getCircuitBreakerTrippedCount(zone);
        }
        return count;
    }
    
    public long getMeasuredZoneHits(String zone) {
        if (zone == null) {
            return 0;
        }
        zone = zone.toLowerCase();
        long count = 0;
        List<? extends Server> currentList = upServerListZoneMap.get(zone);
        if (currentList == null) {
            return 0;
        }
        for (Server server: currentList) {
            ServerStats stat = getSingleServerStat(server);
            count += stat.getMeasuredRequestsCount();
        }
        return count;
    }
        
    public int getCongestionRatePercentage(String zone) {
        if (zone == null) {
            return 0;
        }
        zone = zone.toLowerCase();
        List<? extends Server> currentList = upServerListZoneMap.get(zone);
        if (currentList == null || currentList.size() == 0) {
            return 0;            
        }
        int serverCount = currentList.size(); 
        int activeConnectionsCount = 0;
        int circuitBreakerTrippedCount = 0;
        for (Server server: currentList) {
            ServerStats stat = getSingleServerStat(server);   
            activeConnectionsCount += stat.getActiveRequestsCount();
            if (stat.isCircuitBreakerTripped()) {
                circuitBreakerTrippedCount++;
            }
        }
        return (int) ((activeConnectionsCount + circuitBreakerTrippedCount) * 100L / serverCount); 
    }

    /**
     * 获取存活的 zone，基于 {@link #upServerListZoneMap.keySet}
     * @return
     */
    @Monitor(name=PREFIX + "AvailableZones", type = DataSourceType.INFORMATIONAL)   
    public Set<String> getAvailableZones() {
        return upServerListZoneMap.keySet();
    }

    /**
     * 获取单个服务状态信息
     * @param server 服务
     * @return 服务状态信息
     */
    public ServerStats getSingleServerStat(Server server) {
        // 获取服务状态信息
        return getServerStats(server);
    }

    /**
     * returns map of Stats for all servers
     */
    public Map<Server,ServerStats> getServerStats(){
        return serverStatsCache.asMap();
    }
    
    public Map<String, ZoneStats> getZoneStats() {
        return zoneStatsMap;
    }
    
    @Override
    public String toString() {
        return "Zone stats: " + zoneStatsMap.toString() 
                + "," + "Server stats: " + getSortedServerStats(getServerStats().values()).toString();
    }
    
    private static Comparator<ServerStats> serverStatsComparator = new Comparator<ServerStats>() {
        @Override
        public int compare(ServerStats o1, ServerStats o2) {
            String zone1 = "";
            String zone2 = "";
            if (o1.server != null && o1.server.getZone() != null) {
                zone1 = o1.server.getZone();
            }
            if (o2.server != null && o2.server.getZone() != null) {
                zone2 = o2.server.getZone();
            }
            return zone1.compareTo(zone2);            
        }
    };
    
    private static Collection<ServerStats> getSortedServerStats(Collection<ServerStats> stats) {
        List<ServerStats> list = new ArrayList<ServerStats>(stats);
        Collections.sort(list, serverStatsComparator);
        return list;
    }

}
