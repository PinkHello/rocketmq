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

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 *  生产者 发送消息, 需要指定发送到哪个 MessageQueue, 如果发送失败了，则很可能说明这个 MessageQueue 所在的 Broker 这段时间出现了某种问题,
 *  则在发送下一条消息或者重试的时候, 需要尽可能的规避掉上次发送失败的 broker。
 *
 * MQFaultStrategy
 *  - 挑选 MessageQueue
 *  - 判断开启了故障延迟机制, 则在消息发送失败的时候, 生成 FaultItem 标记这个 Broker 故障
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();

    /**
     * LatencyFaultTolerance 组件 故障延迟机制实现
     */
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    /**
     * 故障延迟机制开启标记
     */
    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 在 rocketMQ 中 一个 Topic 包含多个 MessageQueue, 分散在不同的 Broker。
     *
     * 一个 Producer 发送消息的时候顺序是这样的
     *  - 1. Producer 获取消息路由信息( 当本地缓存无路由信息时候、从 NameSrv 获取路由信息 )
     *  - 2. Producer 发送消息到 Broker
     *  - 3. Broker 存储发送的消息  ( 根据不同 Broker 配置, 可同步或异步存储发送消息 )
     *  - 4. Broker 向 Producer 反馈的发送消息的结果
     *
     *  在这个发送过程中可能存在发送失败
     *  队列的选择和容错策略
     *  - 在不开启容错下, 轮询队列进行发送, 如果失败了, 重试的时候过滤失败的 Broker
     *  - 如果开启了容错, 会通过 RocketMQ 的预测机制来预测一个 Broker 是否可用
     *  - 如果上次失败的 Broker 可用, 那么还继续选择该 Broker 队列
     *  - 如果上述失败, 则随机选择一个 Broker 进行发送
     *  - 在发送消息的时候会记录一下调用的时间与发送是否错误，根据时间去预测 故障的Broker 可用时间
     *
     * @param tpInfo
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 是否开启容错策略
        if (this.sendLatencyFaultEnable) {
            try {
                //选择一个队列, topicInfo ThreadLocalIndex 都 + 1
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                //与队列的长度取模, 根据最后的 pos 取一个队列   轮询找到一个低延迟的Queue
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //判断获取到的队列的Broker 是否在故障列表中, 非故障的则返回
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }
                //代码走到这边, 说明没有非故障的队列 只能从故障列表中获取一个队列
                //如果这个 topic 下的所有的队列都是故障的话, 那么就从故障列表中取出一个 Broker
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                //获取这个 Broker 的可写队列数, 如果该 Broker 没有可写的队列, 则返回 -1
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    // 选择队列, 通过与队列的长度取模确定队列的位置
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    //没有可写的队列,直接从故障列表中移除
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            //如果故障列表中没有可写的队列, 直接从Topic Publish Info中取一个
            return tpInfo.selectOneMessageQueue();
        }
        // 如果没有开启故障延迟, 直接从 Topic Publish Info 通过取模的方式获取队列, 直接走轮询模式
        // 如果LastBrokerName不为空，则需要过滤掉brokerName=lastBrokerName的队列
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        //如果开启了故障延迟机制
        if (this.sendLatencyFaultEnable) {
            //1. 计算故障延迟时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            //2. 更新故障记录的 Table
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算故障延迟的时间
     *     private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
     *     private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};
     *
     *  根据本次发送消息的时间, 选择一个对应的故障延迟时间
     *
     * @param currentLatency
     * @return
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
