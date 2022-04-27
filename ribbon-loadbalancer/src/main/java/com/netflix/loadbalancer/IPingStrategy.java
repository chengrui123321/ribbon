package com.netflix.loadbalancer;

/**
 * Defines the strategy, used to ping all servers, registered in
 * <b>com.netflix.loadbalancer.BaseLoadBalancer</b>. You would
 * typically create custom implementation of this interface, if you
 * want your servers to be pinged in parallel. <b>Please note,
 * that implementations of this interface should be immutable.</b>
 *
 * @author Dmitry_Cherkas
 * @see Server
 * @see IPing
 *
 * Ping 服务策略，可以实现此接口来自定义策略(并发)
 */
public interface IPingStrategy {

    /**
     * ping 服务列表
     * @param ping
     * @param servers
     * @return
     */
    boolean[] pingServers(IPing ping, Server[] servers);
}
