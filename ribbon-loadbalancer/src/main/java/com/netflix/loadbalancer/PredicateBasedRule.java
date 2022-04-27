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

import com.google.common.base.Optional;

/**
 * A rule which delegates the server filtering logic to an instance of {@link AbstractServerPredicate}.
 * After filtering, a server is returned from filtered list in a round robin fashion.
 *
 * @author awang
 *
 * 将服务器过滤逻辑委托给 {@link AbstractServerPredicate} 实例的规则。
 * 过滤后，以循环方式从过滤列表中返回一个服务器
 */
public abstract class PredicateBasedRule extends ClientConfigEnabledRoundRobinRule {

    /**
     * Method that provides an instance of {@link AbstractServerPredicate} to be used by this class.
     *
     * 获取断言器
     */
    public abstract AbstractServerPredicate getPredicate();

    /**
     * Get a server by calling {@link AbstractServerPredicate#chooseRandomlyAfterFiltering(java.util.List, Object)}.
     * The performance for this method is O(n) where n is number of servers to be filtered.
     *
     * 通过 {@link AbstractServerPredicate#chooseRandomlyAfterFiltering(java.util.List, Object)} 来过滤服务列表, 选择一个服务
     */
    @Override
    public Server choose(Object key) {
        // 获取 LoadBalancer
        ILoadBalancer lb = getLoadBalancer();
        /*
            选择服务：
            1. getPredicate()：获取所有断言器
            2. chooseRoundRobinAfterFiltering：根据断言规则进行过滤（传入所有的服务），并从服务列表中选择一个服务
         */
        Optional<Server> server = getPredicate().chooseRoundRobinAfterFiltering(lb.getAllServers(), key);
        if (server.isPresent()) {
            return server.get();
        } else {
            return null;
        }
    }
}
