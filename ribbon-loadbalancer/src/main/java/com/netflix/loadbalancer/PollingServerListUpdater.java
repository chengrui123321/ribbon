package com.netflix.loadbalancer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A default strategy for the dynamic server list updater to update.
 * (refactored and moved here from {@link com.netflix.loadbalancer.DynamicServerListLoadBalancer})
 *
 * @author David Liu
 *
 * 默认方式加载动态服务列表，基于定时调度方式
 */
public class PollingServerListUpdater implements ServerListUpdater {

    private static final Logger logger = LoggerFactory.getLogger(PollingServerListUpdater.class);

    /**
     * 定时延时时间，默认 1 s
     */
    private static long LISTOFSERVERS_CACHE_UPDATE_DELAY = 1000; // msecs;
    /**
     * 定时时间间隔，默认 30 s
     */
    private static int LISTOFSERVERS_CACHE_REPEAT_INTERVAL = 30 * 1000; // msecs;
    private static int POOL_SIZE = 2;

    private static class LazyHolder {
        static ScheduledExecutorService _serverListRefreshExecutor = null;

        static {
            _serverListRefreshExecutor = Executors.newScheduledThreadPool(POOL_SIZE, new ThreadFactoryBuilder()
                    .setNameFormat("PollingServerListUpdater-%d")
                    .setDaemon(true)
                    .build());
        }
    }

    private static ScheduledExecutorService getRefreshExecutor() {
        return LazyHolder._serverListRefreshExecutor;
    }

    /**
     * 是否正在更新，默认 false，并发处理
     */
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    /**
     * 最后更新时间戳
     */
    private volatile long lastUpdated = System.currentTimeMillis();
    /**
     * 初始化延迟时间
     */
    private final long initialDelayMs;
    /**
     * 刷新时间间隔
     */
    private final long refreshIntervalMs;

    /**
     * 定时调度
     */
    private volatile ScheduledFuture<?> scheduledFuture;

    public PollingServerListUpdater() {
        this(LISTOFSERVERS_CACHE_UPDATE_DELAY, LISTOFSERVERS_CACHE_REPEAT_INTERVAL);
    }

    public PollingServerListUpdater(IClientConfig clientConfig) {
        this(LISTOFSERVERS_CACHE_UPDATE_DELAY, getRefreshIntervalMs(clientConfig));
    }

    public PollingServerListUpdater(final long initialDelayMs, final long refreshIntervalMs) {
        this.initialDelayMs = initialDelayMs;
        this.refreshIntervalMs = refreshIntervalMs;
    }

    /**
     * 开启动态服务更新
     * @param updateAction
     *
     */
    @Override
    public synchronized void start(final UpdateAction updateAction) {
        // 如果此时没有线程更新，则执行更新操作
        if (isActive.compareAndSet(false, true)) {
            final Runnable wrapperRunnable = () ->  {
                if (!isActive.get()) {
                    if (scheduledFuture != null) {
                        scheduledFuture.cancel(true);
                    }
                    return;
                }
                try {
                    // 执行具体任务
                    updateAction.doUpdate();
                    // 记录执行时间
                    lastUpdated = System.currentTimeMillis();
                } catch (Exception e) {
                    logger.warn("Failed one update cycle", e);
                }
            };
            // 创建延时调度器，延迟 1 s 执行，每隔 30 s 执行一次
            scheduledFuture = getRefreshExecutor().scheduleWithFixedDelay(
                    wrapperRunnable,
                    initialDelayMs,
                    refreshIntervalMs,
                    TimeUnit.MILLISECONDS
            );
        } else {
            logger.info("Already active, no-op");
        }
    }

    @Override
    public synchronized void stop() {
        if (isActive.compareAndSet(true, false)) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }
        } else {
            logger.info("Not active, no-op");
        }
    }

    @Override
    public String getLastUpdate() {
        return new Date(lastUpdated).toString();
    }

    @Override
    public long getDurationSinceLastUpdateMs() {
        return System.currentTimeMillis() - lastUpdated;
    }

    @Override
    public int getNumberMissedCycles() {
        if (!isActive.get()) {
            return 0;
        }
        return (int) ((int) (System.currentTimeMillis() - lastUpdated) / refreshIntervalMs);
    }

    @Override
    public int getCoreThreads() {
        return POOL_SIZE;
    }

    private static long getRefreshIntervalMs(IClientConfig clientConfig) {
        return clientConfig.get(CommonClientConfigKey.ServerListRefreshInterval, LISTOFSERVERS_CACHE_REPEAT_INTERVAL);
    }
}
