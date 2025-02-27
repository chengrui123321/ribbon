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
 * This interface allows for filtering the configured or dynamically obtained
 * List of candidate servers with desirable characteristics.
 * 
 * @author stonse
 * 
 * @param <T>
 *
 * 服务列表过滤器，用于过滤配置的或动态加载的服务列表
 */
public interface ServerListFilter<T extends Server> {

    /**
     * 过滤服务列表
     * @param servers 服务列表
     * @return 过滤后的服务列表
     */
    public List<T> getFilteredListOfServers(List<T> servers);

}
