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

import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.client.config.IClientConfigKey;
import com.netflix.client.config.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

/**
 * A server predicate that filters out all servers in a worst zone if the aggregated metric for that zone reaches a threshold.
 * The logic to determine the worst zone is described in class {@link ZoneAwareLoadBalancer}.  
 * 
 * @author awang
 * 过滤掉区域中最差服务
 * 最差服务判断逻辑在 {@link ZoneAwareLoadBalancer}
 */
public class ZoneAvoidancePredicate extends AbstractServerPredicate {

    private static final Logger logger = LoggerFactory.getLogger(ZoneAvoidancePredicate.class);

    private static final IClientConfigKey<Double> TRIGGERING_LOAD_PER_SERVER_THRESHOLD = new CommonClientConfigKey<Double>(
            "ZoneAwareNIWSDiscoveryLoadBalancer.%s.triggeringLoadPerServerThreshold", 0.2d) {};

    private static final IClientConfigKey<Double> AVOID_ZONE_WITH_BLACKOUT_PERCENTAGE = new CommonClientConfigKey<Double>(
            "ZoneAwareNIWSDiscoveryLoadBalancer.%s.avoidZoneWithBlackoutPercetage", 0.99999d) {};

    /**
     * 是否开启区域断言， 默认 true
     */
    private static final IClientConfigKey<Boolean> ENABLED = new CommonClientConfigKey<Boolean>(
            "niws.loadbalancer.zoneAvoidanceRule.enabled", true) {};

    private Property<Double> triggeringLoad = Property.of(TRIGGERING_LOAD_PER_SERVER_THRESHOLD.defaultValue());

    private Property<Double> triggeringBlackoutPercentage = Property.of(AVOID_ZONE_WITH_BLACKOUT_PERCENTAGE.defaultValue());

    /**
     * 是否开启区域断言
     */
    private Property<Boolean> enabled = Property.of(ENABLED.defaultValue());

    public ZoneAvoidancePredicate(IRule rule, IClientConfig clientConfig) {
        super(rule);
        initDynamicProperties(clientConfig);
    }

    public ZoneAvoidancePredicate(LoadBalancerStats lbStats, IClientConfig clientConfig) {
        super(lbStats);
        initDynamicProperties(clientConfig);
    }

    ZoneAvoidancePredicate(IRule rule) {
        super(rule);
    }

    private void initDynamicProperties(IClientConfig clientConfig) {
        if (clientConfig != null) {
            enabled = clientConfig.getGlobalProperty(ENABLED);
            triggeringLoad = clientConfig.getGlobalProperty(TRIGGERING_LOAD_PER_SERVER_THRESHOLD.format(clientConfig.getClientName()));
            triggeringBlackoutPercentage = clientConfig.getGlobalProperty(AVOID_ZONE_WITH_BLACKOUT_PERCENTAGE.format(clientConfig.getClientName()));
        }
    }

    /**
     * 应用断言
     * @param input server
     * @return
     */
    @Override
    public boolean apply(@Nullable PredicateKey input) {
        // 如果没有开启区域断言，返回 true, 放行
        if (!enabled.getOrDefault()) {
            return true;
        }
        // 获取服务所在的 zone
        String serverZone = input.getServer().getZone();
        // 应用中没有区分 zone, 返回 true， 放行
        if (serverZone == null) {
            // there is no zone information from the server, we do not want to filter
            // out this server
            return true;
        }
        // 获取负载均衡状态
        LoadBalancerStats lbStats = getLBStats();
        // 负载均衡状态为空，返回 true，放行
        if (lbStats == null) {
            // no stats available, do not filter
            return true;
        }
        // 可用 zone 数量 <= 1，返回 true，放行
        if (lbStats.getAvailableZones().size() <= 1) {
            // only one zone is available, do not filter
            return true;
        }
        // 获取 zone 快照信息
        Map<String, ZoneSnapshot> zoneSnapshot = ZoneAvoidanceRule.createSnapshot(lbStats);
        // 未知的 zone, 返回 true，放行
        if (!zoneSnapshot.keySet().contains(serverZone)) {
            // The server zone is unknown to the load balancer, do not filter it out 
            return true;
        }
        logger.debug("Zone snapshots: {}", zoneSnapshot);
        // 获取所有可用的 zone 集合
        Set<String> availableZones = ZoneAvoidanceRule.getAvailableZones(zoneSnapshot, triggeringLoad.getOrDefault(), triggeringBlackoutPercentage.getOrDefault());
        logger.debug("Available zones: {}", availableZones);
        if (availableZones != null) {
            // 服务 zone 在可用 zone 集合中，匹配上，返回 true
            return availableZones.contains(input.getServer().getZone());
        } else {
            return false;
        }
    }    
}
