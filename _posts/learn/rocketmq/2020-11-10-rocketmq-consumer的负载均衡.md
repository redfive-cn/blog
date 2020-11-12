---
title: rocketmq consumer的负载均衡
layout: article
tags: learn rocketmq
---

在RocketMQ中，Consumer端的两种消费模式（Push/Pull）都是基于拉模式来获取消息的，而在Push模式只是对pull模式的一种封装，其本质实现为消息拉取线程在从服务器拉取到一批消息后，然后提交到消息消费线程池后，又“马不停蹄”的继续向服务器再次尝试拉取消息。如果未拉取到消息，则延迟一下又继续拉取。在两种基于拉模式的消费方式（Push/Pull）中，均需要Consumer端在知道从Broker端的哪一个消息队列—队列中去获取消息。因此，有必要在Consumer端来做负载均衡，即Broker端中多个MessageQueue分配给同一个ConsumerGroup中的哪些Consumer消费。

## consumer的心跳包
在Consumer启动后，consumer会通过定时任务不断地向RocketMQ集群中的所有Broker实例发送心跳包（其中包含了，消息消费分组名称、订阅关系集合、消息通信模式和客户端id的值等信息）。
Broker端在收到Consumer的心跳消息后，会将它维护在ConsumerManager的本地缓存变量—consumerTable，同时并将封装后的客户端网络通道信息保存在本地缓存变量—channelInfoTable中，为之后做Consumer端的负载均衡提供可以依据的元数据信息。

## Consumer端实现负载均衡的核心类—RebalanceImpl

![consumer负载均衡图解](/img/learn/rocketmq/mq-consumer-rebalance-diagram.png)

>com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.factory.MQClientInstance

```java
public class MQClientInstance {

    public void start() throws MQClientException {
            synchronized (this) {
                switch (this.serviceState) {
                    case CREATE_JUST:
                        this.serviceState = ServiceState.START_FAILED;
                        // If not specified,looking address from name server
                        if (null == this.clientConfig.getNamesrvAddr()) {
                            this.mQClientAPIImpl.fetchNameServerAddr();
                        }
                        // Start request-response channel
                        this.mQClientAPIImpl.start();
                        // Start various schedule tasks
                        this.startScheduledTask();
                        // Start pull service
                        this.pullMessageService.start();
                        // Start rebalance service
                        this.rebalanceService.start();
                        // Start push service
                        this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                        log.info("the client factory [{}] start OK", this.clientId);
                        this.serviceState = ServiceState.RUNNING;
                        break;
                    case RUNNING:
                        break;
                    case SHUTDOWN_ALREADY:
                        break;
                    case START_FAILED:
                        throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                    default:
                        break;
                }
            }
        }

        public boolean doRebalance() {
                boolean balanced = true;
                for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                    MQConsumerInner impl = entry.getValue();
                    if (impl != null) {
                        try {
                            if (!impl.doRebalance()) {
                                balanced = false;
                            }
                        } catch (Throwable e) {
                            log.error("doRebalance exception", e);
                        }
                    }
                }

                return balanced;
            }

}
```

>org.apache.rocketmq.client.impl.consumer.RebalanceService

```java
public class RebalanceService extends ServiceThread {
    private static long waitInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.waitInterval", "20000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            this.waitForRunning(waitInterval);
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
```

如上所示，`com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.factory.MQClientInstance#start`中会启动负载均衡服务。
负载均衡服务中，默认每隔20s调用一次`com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.factory.MQClientInstance#doRebalance`。
主要逻辑`com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.RebalanceImpl`中完成。

>com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.RebalanceImpl

```java
public abstract class RebalanceImpl {

    public boolean doRebalance(final boolean isOrder) {
            boolean balanced = true;
            Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
            if (subTable != null) {
                for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                    final String topic = entry.getKey();
                    try {
                        if (!this.rebalanceByTopic(topic, isOrder)) {
                            balanced = false;
                        }
                    } catch (Throwable e) {
                        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                            log.warn("rebalanceByTopic Exception", e);
                        }
                    }
                }
            }

            this.truncateMessageQueueNotMyTopic();

            return balanced;
        }

    private boolean rebalanceByTopic(final String topic, final boolean isOrder) {
            boolean balanced = true;
            switch (messageModel) {
                case BROADCASTING: {
                    Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                    if (mqSet != null) {
                        boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                        if (changed) {
                            this.messageQueueChanged(topic, mqSet, mqSet);
                            log.info("messageQueueChanged {} {} {} {}", consumerGroup, topic, mqSet, mqSet);
                        }

                        balanced = mqSet.equals(getWorkingMessageQueue(topic));
                    } else {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                    break;
                }
                case CLUSTERING: {
                    Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                    List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                    if (null == mqSet) {
                        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                            log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                        }
                    }

                    if (null == cidAll) {
                        log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                    }

                    if (mqSet != null && cidAll != null) {
                        List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                        mqAll.addAll(mqSet);

                        Collections.sort(mqAll);
                        Collections.sort(cidAll);

                        AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                        List<MessageQueue> allocateResult = null;
                        try {
                            allocateResult = strategy.allocate(
                                this.consumerGroup,
                                this.mQClientFactory.getClientId(),
                                mqAll,
                                cidAll);
                        } catch (Throwable e) {
                            log.error("allocate message queue exception. strategy name: {}, ex: {}", strategy.getName(), e);
                            return false;
                        }

                        Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                        if (allocateResult != null) {
                            allocateResultSet.addAll(allocateResult);
                        }

                        boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                        if (changed) {
                            log.info(
                                "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                                strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                                allocateResultSet.size(), allocateResultSet);
                            this.messageQueueChanged(topic, mqSet, allocateResultSet);
                        }

                        balanced = allocateResultSet.equals(getWorkingMessageQueue(topic));
                    }
                    break;
                }
                default:
                    break;
            }

            return balanced;
        }

        private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet, final boolean isOrder) {
                boolean changed = false;

                // drop process queues no longer belong me
                HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<MessageQueue, ProcessQueue>(this.processQueueTable.size());
                Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<MessageQueue, ProcessQueue> next = it.next();
                    MessageQueue mq = next.getKey();
                    ProcessQueue pq = next.getValue();

                    if (mq.getTopic().equals(topic)) {
                        if (!mqSet.contains(mq)) {
                            pq.setDropped(true);
                            removeQueueMap.put(mq, pq);
                        } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                            pq.setDropped(true);
                            removeQueueMap.put(mq, pq);
                            log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                consumerGroup, mq);
                        }
                    }
                }

                // remove message queues no longer belong me
                for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
                    MessageQueue mq = entry.getKey();
                    ProcessQueue pq = entry.getValue();

                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        this.processQueueTable.remove(mq);
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                }

                // add new message queue
                boolean allMQLocked = true;
                List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
                for (MessageQueue mq : mqSet) {
                    if (!this.processQueueTable.containsKey(mq)) {
                        if (isOrder && !this.lock(mq)) {
                            log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                            allMQLocked = false;
                            continue;
                        }

                        this.removeDirtyOffset(mq);
                        ProcessQueue pq = createProcessQueue();
                        pq.setLocked(true);
                        long nextOffset = this.computePullFromWhere(mq);
                        if (nextOffset >= 0) {
                            ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                            if (pre != null) {
                                log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                            } else {
                                log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                                PullRequest pullRequest = new PullRequest();
                                pullRequest.setConsumerGroup(consumerGroup);
                                pullRequest.setNextOffset(nextOffset);
                                pullRequest.setMessageQueue(mq);
                                pullRequest.setProcessQueue(pq);
                                pullRequestList.add(pullRequest);
                                changed = true;
                            }
                        } else {
                            log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                        }
                    }
                }

                if (!allMQLocked) {
                    mQClientFactory.rebalanceLater(500);
                }

                this.dispatchPullRequest(pullRequestList, 500);

                return changed;
            }

}
```

在`com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.RebalanceImpl`中，负载均衡的主要逻辑在`rebalanceByTopic`方法中。

对于集群模式下：

1.先在本地的`topicSubscribeInfoTable`中找到该topic下的消息队列集合。

2.向broker请求消费者id列表

3.使用`AllocateMessageQueueStrategy`计算出新的messageQueue

4.移除processQueue:在`updateProcessQueueTableInRebalance`方法中先计算出需要移除的mq，再进行移除：上报至broker并移除本地的`processQueueTable`中的项。

5.添加processQueue:在`updateProcessQueueTableInRebalance`方法中，对于需要添加的processQueue，先用`LOCK_BATCH_MQ`请求对queue加锁。
若加锁成功，先移除dirtyOffset，然后从broker查上次消费的位点(即`computePullFromWhere()`)，添加到PullRequestList中。
为每个MessageQueue创建一个ProcessQueue对象并存入RebalanceImpl的processQueueTable队列中。

6.执行`dispatchPullRequest()`方法，将Pull消息的请求对象PullRequest依次放入PullMessageService服务线程的阻塞队列pullRequestQueue中，待该服务线程取出后向Broker端发起Pull消息的请求。
其中，可以重点对比下，RebalancePushImpl和RebalancePullImpl两个实现类的dispatchPullRequest()方法不同，RebalancePullImpl类里面的该方法为空。原因要到两种消费方式原理中找。

>消息消费队列在同一消费组不同消费者之间的负载均衡，其核心设计理念是在一个消息消费队列在同一时间只允许被同一消费组内的一个消费者消费，一个消息消费者能同时消费多个消息队列。

>参考:[官方文档:rocketMq设计](https://github.com/apache/rocketmq/tree/master/docs/cn/design.md)
