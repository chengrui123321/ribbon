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

import java.util.List;

/**
 * AbstractLoadBalancer contains features required for most loadbalancing
 * implementations.
 * 
 * An anatomy of a typical LoadBalancer consists of 1. A List of Servers (nodes)
 * that are potentially bucketed based on a specific criteria. 2. A Class that
 * defines and implements a LoadBalacing Strategy via <code>IRule</code> 3. A
 * Class that defines and implements a mechanism to determine the
 * suitability/availability of the nodes/servers in the List.
 * 
 * @author stonse
 *
 * 负载均衡器抽象基类，提供基本方法
 * 
 */
public abstract class AbstractLoadBalancer implements ILoadBalancer {

    /**
     * 服务组
     */
    public enum ServerGroup{
        /**
         * 全部
         */
        ALL,
        /**
         * 活跃
         */
        STATUS_UP,
        /**
         * 下线
         */
        STATUS_NOT_UP        
    }
        
    /**
     * delegate to {@link #chooseServer(Object)} with parameter null.
     *
     * 选择一个服务，由子类实现
     */
    public Server chooseServer() {
    	return chooseServer(null);
    }

    
    /**
     * List of servers that this Loadbalancer knows about
     * 
     * @param serverGroup Servers grouped by status, e.g., {@link ServerGroup#STATUS_UP}
     *
     * 获取指定分组的服务列表，由子类实现
     */
    public abstract List<Server> getServerList(ServerGroup serverGroup);
    
    /**
     * Obtain LoadBalancer related Statistics
     *
     * 负载均衡器状态
     */
    public abstract LoadBalancerStats getLoadBalancerStats();    
}
