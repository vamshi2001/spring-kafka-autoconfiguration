package com.api.hub.kafka.pojo;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import lombok.Data;

@Data
public class KafkaListenerContainerConfigProperties {
	
	private String containerName;

	/***
     * The acknowledgment mode for message consumption.
     * Determines how offsets are committed after message processing.
     */
    private ContainerProperties.AckMode ackMode = AckMode.MANUAL_IMMEDIATE;

    /***
     * The maximum time (in milliseconds) the consumer will block
     * while polling for new records.
     */
    private Long pollTimeout;

    /***
     * Time interval (in milliseconds) for emitting idle container events
     * when no records are received.
     */
    private Long idleEventInterval;

    /***
     * Whether the container should fail to start if any configured topic is missing.
     */
    private boolean missingTopicsFatal;

    /***
     * Whether to commit the offsets synchronously after each message or batch is processed.
     * Can be used to ensure commits are not lost in case of failure.
     */
    private boolean syncCommits;

    /***
     * The time (in milliseconds) to wait between successive polls.
     * Helps throttle the consumer and reduce unnecessary polling.
     */
    private Long idleBetweenPolls;

    /***
     * An optional client ID to assign to the Kafka consumer.
     * Helps identify consumers in logs and monitoring tools.
     */
    private String clientId;

    /***
     * Whether to log the full container configuration on startup.
     * Useful for debugging and verification purposes.
     */
    private boolean logContainerConfig;

    /***
     * Whether to include the delivery attempt count in the record headers.
     * Useful for error handling and retry logic.
     */
    private boolean deliveryAttemptHeader;
    
    private boolean batchMode;
    
    private boolean fixedBackOff;
    private long fixedBackOffIntervel;
    private int fixedBackOffMaxRetries;
    
    private long backOffInitialInterval;
    
    private double backOffMultiplier;

    private long backOffMaxInterval;
    
    private String recoverBeanName;
    
    // start of consumerProperties
    
    /** The maximum number of records returned in a single poll request.
     * Corresponds to: ConsumerConfig.MAX_POLL_RECORDS_CONFIG
     */
    private int maxPollRecords;

    /** The maximum delay between invocations of poll() before the consumer is considered failed.
     * Corresponds to: ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG
     */
    private int maxPollInterval;

    /** The timeout used to detect consumer failures when using Kafka's group management.
     * Must be >= group.min.session.timeout.ms && <= group.max.session.timeout.ms in broker config.
     * Corresponds to: ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG
     */
    private int maxSessionTimeout;

    /** The expected time between heartbeats to the consumer group coordinator.
     * Corresponds to: ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG
     */
    private int heartBeatInterval;

    /** The protocol type for the consumer group (e.g., "consumer").
     * Corresponds to: ConsumerConfig.GROUP_PROTOCOL_CONFIG
     */
    private String groupProtocol;

    /** The class name of a custom remote assignor used during partition assignment.
     * Corresponds to: ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG
     */
    private String groupRemoteAssignor;

    /** Comma-separated list of Kafka broker addresses.
     * Corresponds to: ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
     */
    private String bootStrapServers;

    /** If true, the consumer's offset will be periodically committed in the background.
     * Corresponds to: ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
     */
    private boolean autoCommit = false;

    /** A list of partition assignment strategy class names used to distribute topic partitions across consumers.
     * Corresponds to: ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG
     */
    private String partitionAssignmentStarategy =
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor,"
            + "org.apache.kafka.clients.consumer.RangeAssignor,"
            + "org.apache.kafka.clients.consumer.RoundRobinAssignor";

    /** What to do when there is no initial offset or the current offset no longer exists on the server.
     * Typical values: "latest", "earliest"
     * Corresponds to: ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
     */
    private String autoOffsetReset = "earliest";

    /** Minimum amount of data the server should return for a fetch request.
     * Corresponds to: ConsumerConfig.FETCH_MIN_BYTES_CONFIG
     */
    private int fetchMinBytes;

    /** Maximum amount of data the server should return for a fetch request.
     * Corresponds to: ConsumerConfig.FETCH_MAX_BYTES_CONFIG
     */
    private int fetchMaxBytes;

    /** Maximum time the server will block before answering the fetch request if there isn’t sufficient data.
     * Corresponds to: ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG
     */
    private int fetchMaxWaitTime;

    /** Maximum amount of data per partition the server will return in a fetch request.
     * Corresponds to: ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG
     */
    private int fetchMaxBytesPerPartition;

    /** The size of the TCP send buffer (SO_SNDBUF) to use when sending data.
     * Corresponds to: ConsumerConfig.SEND_BUFFER_CONFIG
     */
    private int sendBuffer;

    /** The size of the TCP receive buffer (SO_RCVBUF) to use when reading data.
     * Corresponds to: ConsumerConfig.RECEIVE_BUFFER_CONFIG
     */
    private int receiveBuffer;

    /** Whether to verify CRCs when reading records from Kafka.
     * Corresponds to: ConsumerConfig.CHECK_CRCS_CONFIG
     */
    private boolean crcCheck;

    /** Deserializer class for keys in Kafka records.
     * Corresponds to: ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
     */
    private String keyDeserializer;

    /** Deserializer class for values in Kafka records.
     * Corresponds to: ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
     */
    private String valueDeserializer;

    /** Close idle connections after this number of milliseconds.
     * Corresponds to: ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG
     */
    private long maxIdleConnectionTimeOut;

    /** Maximum amount of time the client will wait for the response of a request.
     * Corresponds to: ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG
     */
    private int requestTimeout;

    /** Optional username for SASL authentication (used in secured clusters).
     * Not directly mapped to ConsumerConfig; used in security configuration.
     */
    private String userName;

    /** Optional password for SASL authentication (used in secured clusters).
     * Not directly mapped to ConsumerConfig; used in security configuration.
     */
    private String password;

    
    // ...getters and setters for all above fields
    
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        map.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStarategy);
        map.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
        map.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, groupRemoteAssignor); // Custom or advanced config
        map.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        map.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        map.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, maxSessionTimeout);
        map.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartBeatInterval);
        map.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        map.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        map.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitTime);
        map.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, fetchMaxBytesPerPartition); // Not defined in ConsumerConfig
        map.put(ConsumerConfig.SEND_BUFFER_CONFIG, sendBuffer);
        map.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, receiveBuffer);
        map.put(ConsumerConfig.CHECK_CRCS_CONFIG, crcCheck);
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        map.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, maxIdleConnectionTimeOut);
        map.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        
        
        String saslJassConfig = new StringBuilder("org.apache.kafka.common.security.plain.PlainLoginModule required username= '")
				.append(userName)
				.append("' password= '")
				.append(password)
				.append("' ;")
				.toString();
        
        map.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        map.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
		map.put(SaslConfigs.SASL_JAAS_CONFIG, saslJassConfig);

        return map;
    }
}
