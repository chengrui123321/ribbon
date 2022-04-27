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

import javax.annotation.Nullable;

/**
 * Predicate with the logic of filtering out circuit breaker tripped servers and servers 
 * with too many concurrent connections from this client.
 * 
 * @author awang
 *
 * 过滤熔断状态的服务及并发过高的服务
 */
public class AvailabilityPredicate extends  AbstractServerPredicate {

    /**
     * 是否需要过滤，默认 true
     */
    private static final IClientConfigKey<Boolean> FILTER_CIRCUIT_TRIPPED = new CommonClientConfigKey<Boolean>(
            "niws.loadbalancer.availabilityFilteringRule.filterCircuitTripped", true) {};

    /**
     * 默认服务连接数限制，默认 -1
     */
    private static final IClientConfigKey<Integer> DEFAULT_ACTIVE_CONNECTIONS_LIMIT = new CommonClientConfigKey<Integer>(
            "niws.loadbalancer.availabilityFilteringRule.activeConnectionsLimit", -1) {};

    /**
     * 存活连接数量限制，默认 -1
     */
    private static final IClientConfigKey<Integer> ACTIVE_CONNECTIONS_LIMIT = new CommonClientConfigKey<Integer>(
            "ActiveConnectionsLimit", -1) {};

    /**
     * 是否需要过滤
     */
    private Property<Boolean> circuitBreakerFiltering = Property.of(FILTER_CIRCUIT_TRIPPED.defaultValue());

    /**
     * 默认服务连接数限制
     */
    private Property<Integer> defaultActiveConnectionsLimit = Property.of(DEFAULT_ACTIVE_CONNECTIONS_LIMIT.defaultValue());
    /**
     * 存活连接数量限制
     */
    private Property<Integer> activeConnectionsLimit = Property.of(ACTIVE_CONNECTIONS_LIMIT.defaultValue());

    public AvailabilityPredicate(IRule rule, IClientConfig clientConfig) {
        super(rule);
        initDynamicProperty(clientConfig);
    }

    public AvailabilityPredicate(LoadBalancerStats lbStats, IClientConfig clientConfig) {
        super(lbStats);
        initDynamicProperty(clientConfig);
    }

    AvailabilityPredicate(IRule rule) {
        super(rule);
    }

    private void initDynamicProperty(IClientConfig clientConfig) {
        if (clientConfig != null) {
            this.circuitBreakerFiltering = clientConfig.getGlobalProperty(FILTER_CIRCUIT_TRIPPED);
            this.defaultActiveConnectionsLimit = clientConfig.getGlobalProperty(DEFAULT_ACTIVE_CONNECTIONS_LIMIT);
            this.activeConnectionsLimit = clientConfig.getDynamicProperty(ACTIVE_CONNECTIONS_LIMIT);
        }
    }

    private int getActiveConnectionsLimit() {
        Integer limit = activeConnectionsLimit.getOrDefault();
        if (limit == -1) {
            limit = defaultActiveConnectionsLimit.getOrDefault();
            if (limit == -1) {
                limit = Integer.MAX_VALUE;
            }
        }
        return limit;
    }

    /**
     * 过滤熔断状态下的服务及并发连接过多的服务
     * @param input server
     * @return
     */
    @Override
    public boolean apply(@Nullable PredicateKey input) {
        // 获取负载均衡器状态
        LoadBalancerStats stats = getLBStats();
        if (stats == null) {
            return true;
        }
        // 是否需要跳过该服务
        return !shouldSkipServer(stats.getSingleServerStat(input.getServer()));
    }

    /**
     * 是否需要跳过该服务，跳过则直接应用成功
     * @param stats 服务状态
     * @return
     */
    private boolean shouldSkipServer(ServerStats stats) {
        if ((circuitBreakerFiltering.getOrDefault() //niws.loadbalancer.availabilityFilteringRule.filterCircuitTripped 是否为 true
                && stats.isCircuitBreakerTripped()) //该 Server 是否为断路状态
                || stats.getActiveRequestsCount() >= getActiveConnectionsLimit()) { //本机发往这个 Server 未处理完的请求个数是否大于 Server 实例最大的活跃连接数
            return true;
        }
        return false;
    }

}
