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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The most well known and basic load balancing strategy, i.e. Round Robin Rule.
 *
 * @author stonse
 * @author <a href="mailto:nikos@netflix.com">Nikos Michalakis</a>
 *
 * 轮询负载均衡策略
 */
public class RoundRobinRule extends AbstractLoadBalancerRule {

    /**
     * 循环计数器
     */
    private AtomicInteger nextServerCyclicCounter;
    /**
     * 仅可用服务
     */
    private static final boolean AVAILABLE_ONLY_SERVERS = true;
    /**
     * 全部服务
     */
    private static final boolean ALL_SERVERS = false;

    private static Logger log = LoggerFactory.getLogger(RoundRobinRule.class);

    public RoundRobinRule() {
        // 初始化计数器
        nextServerCyclicCounter = new AtomicInteger(0);
    }

    public RoundRobinRule(ILoadBalancer lb) {
        this();
        setLoadBalancer(lb);
    }

    /**
     * 选择服务
     * @param lb 负载均衡器
     * @param key key
     * @return Server
     */
    public Server choose(ILoadBalancer lb, Object key) {
        // 如果 LoadBalancer 为空，直接返回 null
        if (lb == null) {
            log.warn("no load balancer");
            return null;
        }
        // 服务
        Server server = null;
        int count = 0;
        // 如果服务为空，并且 数量 < 10, 循环获取服务
        while (server == null && count++ < 10) {
            // 获取活跃的服务
            List<Server> reachableServers = lb.getReachableServers();
            // 获取所有服务
            List<Server> allServers = lb.getAllServers();
            // 活跃服务数量
            int upCount = reachableServers.size();
            // 所有服务数量
            int serverCount = allServers.size();
            // 如果活跃服务数量为 0 或者 全部服务数量为 0， 返回 null
            if ((upCount == 0) || (serverCount == 0)) {
                log.warn("No up servers available from load balancer: " + lb);
                return null;
            }
            // 获取下一个服务索引
            int nextServerIndex = incrementAndGetModulo(serverCount);
            // 获取服务
            server = allServers.get(nextServerIndex);
            // 如果为空，线程等待，并且跳过本次循环
            if (server == null) {
                /* Transient. */
                Thread.yield();
                continue;
            }
            // 如果服务是存活，并且可用，直接返回
            if (server.isAlive() && (server.isReadyToServe())) {
                return (server);
            }

            // Next.
            server = null;
        }
        // 如果循环查找次数大于 10，记录日志
        if (count >= 10) {
            log.warn("No available alive servers after 10 tries from load balancer: "
                    + lb);
        }
        return server;
    }

    /**
     * Inspired by the implementation of {@link AtomicInteger#incrementAndGet()}.
     *
     * @param modulo The modulo to bound the value of the counter.
     * @return The next value.
     */
    private int incrementAndGetModulo(int modulo) {
        for (;;) {
            int current = nextServerCyclicCounter.get();
            int next = (current + 1) % modulo;
            if (nextServerCyclicCounter.compareAndSet(current, next))
                return next;
        }
    }

    /**
     * 选择服务
     * @param key key
     * @return Server
     */
    @Override
    public Server choose(Object key) {
        return choose(getLoadBalancer(), key);
    }
}
