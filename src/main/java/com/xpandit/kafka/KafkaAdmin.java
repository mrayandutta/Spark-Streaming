package com.xpandit.kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public class KafkaAdmin implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdmin.class);

    private static final int ZK_SESSION_TIMEOUT_MS = 6000;
    private static final int ZK_CONNECTION_TIMEOUT_MS = 6000;

    private final ZkClient zkClient;
    private final ZkConnection zkConnection;

    public KafkaAdmin(String zookeeperConnect, String zookeeperPrefix) {
        this(zookeeperConnect, zookeeperPrefix, ZK_SESSION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS);
    }

    public KafkaAdmin(String zookeeperConnect, String zookeeperPrefix, int sessionTimeout, int connectionTimeout) {
        this.zkClient = new ZkClient(zookeeperConnect + zookeeperPrefix, sessionTimeout, connectionTimeout,
                ZKStringSerializer$.MODULE$);
        this.zkConnection = new ZkConnection(zookeeperConnect, sessionTimeout);
    }

    /**
     * @param //topicConfig detailed configuration properties for the topic. Use {@link kafka.log.LogConfig} to get the
     *                      names of the available properties.
     * @return true if the topic is created, false if it already exists.
     */
    /*public boolean createTopic(String topic, int partitions, int replicationFactor, Properties topicConfig) {
        boolean created = true;

        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        MetadataResponse.TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
        if (topicMetadata.errorCode() == ErrorMapping.UnknownTopicOrPartitionCode()) {
            try {
                AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, topicConfig);
                LOGGER.info("Created Kafka topic '{}' with {} partitions and replication factor {}.",
                        topic, partitions, replicationFactor);
            } catch (AdminOperationException e) {
                // TODO retry after an exponential back-off time?
                // In the case a broker has died (or similar) retrying may result, but in the case
                // there are no enough brokers to satisfy the desired replication factor, retrying
                // will not change anything.
                throw new RuntimeException(e);
            } catch (TopicExistsException e) {
                created = false;
            }
        }
        return created;
    }*/
    /*public MetadataResponse.TopicMetadata getTopicMetadata(String topic) {
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        return AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
    }*/
    public MetadataResponse.TopicMetadata getTopicMetadata(String topic) {
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        return AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
    }

    @Override
    public void close() {
        try {
            zkClient.close();
            zkConnection.close();
        } catch (InterruptedException e) {
            LOGGER.error("Failed to close Zookeeper connection", e);
        }
    }
}
