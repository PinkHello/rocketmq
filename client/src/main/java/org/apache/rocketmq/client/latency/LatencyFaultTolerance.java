/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.latency;

/**
 *
 * 故障延迟机制
 *
 * 用于判断一个 broker 是否有啥问题, 有一个默认的实现 LatencyFaultToleranceImpl, 内部有个 faultItemTable(Map), 如果哪一次消息发送失败,
 * 如果开启了故障延迟机制, 则构建一条 faultItem 记录, 以 BrokerName 为 Key 加入到这个 map 中, 那在某个时刻之前, 这个 broker 都是有问题的。
 *
 * 该接口就是干此事件的
 *
 * @param <T>
 */
public interface LatencyFaultTolerance<T> {

    /**
     * 更新故障项
     * @param name
     * @param currentLatency
     * @param notAvailableDuration
     */
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);

    /**
     * 判断某个故障的Broker 是否可用
     * @param name
     * @return
     */
    boolean isAvailable(final T name);

    /**
     * 从故障列表内 删除指的故障的 Broker
     * @param name
     */
    void remove(final T name);

    /**
     * 获取故障项
     * @return
     */
    T pickOneAtLeast();
}
