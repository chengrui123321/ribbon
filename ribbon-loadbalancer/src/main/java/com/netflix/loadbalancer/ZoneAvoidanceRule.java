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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.netflix.client.config.IClientConfig;

/**
 * A rule that uses the a {@link CompositePredicate} to filter servers based on zone and availability. The primary predicate is composed of
 * a {@link ZoneAvoidancePredicate} and {@link AvailabilityPredicate}, with the fallbacks to {@link AvailabilityPredicate}
 * and an "always true" predicate returned from {@link AbstractServerPredicate#alwaysTrue()}
 *
 * @author awang
 * 使用 {@link ZoneAvoidancePredicate} 和 {@link AvailabilityPredicate} 进行 zone 服务过滤
 * <p>
 * 在 spring cloud 中使用此类作为默认策略
 */
public class ZoneAvoidanceRule extends PredicateBasedRule {

    /**
     * 随机数
     */
    private static final Random random = new Random();

    /**
     * 复合断言策略
     */
    private CompositePredicate compositePredicate;

    /**
     * 创建 ZoneAvoidanceRule
     * 复合断言： {@link ZoneAvoidancePredicate} 和 {@link AvailabilityPredicate}，多个断言之间 and 关系
     */
    public ZoneAvoidanceRule() {
        // 创建 ZoneAvoidancePredicate
        ZoneAvoidancePredicate zonePredicate = new ZoneAvoidancePredicate(this);
        // 创建 AvailabilityPredicate
        AvailabilityPredicate availabilityPredicate = new AvailabilityPredicate(this);
        compositePredicate = createCompositePredicate(zonePredicate, availabilityPredicate);
    }

    /**
     * 创建复合的断言规则
     *
     * @param p1 ZoneAvoidancePredicate
     * @param p2 AvailabilityPredicate
     * @return CompositePredicate
     */
    private CompositePredicate createCompositePredicate(ZoneAvoidancePredicate p1, AvailabilityPredicate p2) {
        return CompositePredicate.withPredicates(p1, p2)
                // 回退策略为 AvailabilityPredicate
                .addFallbackPredicate(p2)
                .addFallbackPredicate(AbstractServerPredicate.alwaysTrue())
                .build();
    }

    @Override
    public void initWithNiwsConfig(IClientConfig clientConfig) {
        ZoneAvoidancePredicate zonePredicate = new ZoneAvoidancePredicate(this, clientConfig);
        AvailabilityPredicate availabilityPredicate = new AvailabilityPredicate(this, clientConfig);
        compositePredicate = createCompositePredicate(zonePredicate, availabilityPredicate);
    }

    /**
     * 创建区域快照信息
     *
     * @param lbStats 负载均衡器状态
     * @return
     */
    static Map<String, ZoneSnapshot> createSnapshot(LoadBalancerStats lbStats) {
        Map<String, ZoneSnapshot> map = new HashMap<String, ZoneSnapshot>();
        // lbStats.getAvailableZones() 获取所有存活服务的 zone
        for (String zone : lbStats.getAvailableZones()) {
            // 获取 zone 对应的快照信息
            ZoneSnapshot snapshot = lbStats.getZoneSnapshot(zone);
            map.put(zone, snapshot);
        }
        return map;
    }

    /**
     * 计算算法，随机在可用 zone 中获取一个 zone
     *
     * @param snapshot   zone 快照
     * @param chooseFrom 可用 zone
     * @return 选中的 zone
     */
    static String randomChooseZone(Map<String, ZoneSnapshot> snapshot,
                                   Set<String> chooseFrom) {
        // 如果可用 zone 为空，返回 null
        if (chooseFrom == null || chooseFrom.size() == 0) {
            return null;
        }
        // 选中的 zone
        String selectedZone = chooseFrom.iterator().next();
        // 如果可用 zone 数量为 1，直接返回 zone
        if (chooseFrom.size() == 1) {
            return selectedZone;
        }
        // 服务总数量
        int totalServerCount = 0;
        // 遍历，累加服务数量
        for (String zone : chooseFrom) {
            totalServerCount += snapshot.get(zone).getInstanceCount();
        }
        int index = random.nextInt(totalServerCount) + 1;
        int sum = 0;
        // 随机选择，zone 实例数量越多，概率越大
        for (String zone : chooseFrom) {
            sum += snapshot.get(zone).getInstanceCount();
            if (index <= sum) {
                selectedZone = zone;
                break;
            }
        }
        return selectedZone;
    }

    /**
     * 获取可用的 zone 集合
     * @param snapshot zone 快照信息
     * @param triggeringLoad
     * @param triggeringBlackoutPercentage
     * @return
     */
    public static Set<String> getAvailableZones(
            Map<String, ZoneSnapshot> snapshot, double triggeringLoad,
            double triggeringBlackoutPercentage) {
        // 如果快照信息为空，返回 null
        if (snapshot.isEmpty()) {
            return null;
        }
        // 可用 zone 集合
        Set<String> availableZones = new HashSet<String>(snapshot.keySet());
        // 如果只有 zone  只有 1 个，直接返回该 zone
        if (availableZones.size() == 1) {
            return availableZones;
        }
        // 坏的 zone 集合
        Set<String> worstZones = new HashSet<String>();
        // 定义变量，保存所有 zone 中平均负载最高值
        double maxLoadPerServer = 0;
        // true: zone 有限可用；false: 全部可用
        boolean limitedZoneAvailability = false;
        // 遍历所有快照信息
        for (Map.Entry<String, ZoneSnapshot> zoneEntry : snapshot.entrySet()) {
            // 获取 zone
            String zone = zoneEntry.getKey();
            // 获取 快照信息
            ZoneSnapshot zoneSnapshot = zoneEntry.getValue();
            // 获取 zone 中实例数量
            int instanceCount = zoneSnapshot.getInstanceCount();
            // 如果当前 zone 中实例数量为 0
            if (instanceCount == 0) {
                // 从可用 zone 集合中移除该 zone
                availableZones.remove(zone);
                // 该 zone 有限可用
                limitedZoneAvailability = true;
            } else { // 如果 zone 中实例数量 > 1
                // 获取 zone 的平均负载
                double loadPerServer = zoneSnapshot.getLoadPerServer();
                if (((double) zoneSnapshot.getCircuitTrippedCount()) // 如果熔断服务数量 / 实例总数 >= 0.9999，完全不可用
                        / instanceCount >= triggeringBlackoutPercentage
                        || loadPerServer < 0) { // loadPerServer 表示当前区域所有节点都熔断了
                    // 移除该 zone
                    availableZones.remove(zone);
                    // 有限可用
                    limitedZoneAvailability = true;
                } else { // 此逻辑代表不是完全不可用
                    // 如果当前负载和最大负载相当，那认为当前区域状态很不好，加入到 worstZones 中
                    if (Math.abs(loadPerServer - maxLoadPerServer) < 0.000001d) {
                        // they are the same considering double calculation
                        // round error
                        worstZones.add(zone);
                    } else if (loadPerServer > maxLoadPerServer) { // 或者当前负载 > 最大负载
                        maxLoadPerServer = loadPerServer;
                        worstZones.clear();
                        // 添加到坏 zone 中
                        worstZones.add(zone);
                    }
                }
            }
        }
        // 如果最大负载小于设定的负载阈值 并且 limitedZoneAvailability = false
        // 说明全部 zone 都可用，并且最大负载都还没有达到阈值，那就把全部 zone 返回
        if (maxLoadPerServer < triggeringLoad && !limitedZoneAvailability) {
            // zone override is not needed here
            return availableZones;
        }
        //若最大负载超过阈值， 就不能全部返回，则直接从负载最高的区域中随机返回一个，这么处理的目的是把负载最高的那个哥们T除掉，再返回结果。
        String zoneToAvoid = randomChooseZone(snapshot, worstZones);
        if (zoneToAvoid != null) {
            availableZones.remove(zoneToAvoid);
        }
        return availableZones;

    }

    public static Set<String> getAvailableZones(LoadBalancerStats lbStats,
                                                double triggeringLoad, double triggeringBlackoutPercentage) {
        if (lbStats == null) {
            return null;
        }
        Map<String, ZoneSnapshot> snapshot = createSnapshot(lbStats);
        return getAvailableZones(snapshot, triggeringLoad,
                triggeringBlackoutPercentage);
    }

    /**
     * 返回复合断言器
     * @return 复合断言器
     */
    @Override
    public AbstractServerPredicate getPredicate() {
        return compositePredicate;
    }
}
