package com.api.hub.kafka.pojo;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.api.hub.kafka.common.APIException;

import java.util.List;
import java.util.regex.Pattern;

@Scope("singleton")
@Component("ListernerData")
public class ListenerData {
	
	private String name;

    // Core Attributes

    /** A unique identifier for the listener container. If not specified, a default ID is generated. */
    private String id;

    /** Specifies the Kafka consumer group ID. Overrides the group.id property in the consumer factory. */
    private String groupId;

    /** An array of topic names to subscribe to. */
    private List<String> topics;

    /** The name of the KafkaListenerContainerFactory bean to use. Defaults to kafkaListenerContainerFactory. */
    private String containerFactory;


    // Consumer Configuration

    /** The number of concurrent threads for the listener. Overrides the factory’s concurrency setting. */
    private Integer concurrency;

    /** Determines whether the listener container should start automatically. Defaults to true. */
    private Boolean autoStartup = true;

    /** Prefix for the Kafka consumer client ID. Useful for identifying consumers. */
    private String clientIdPrefix;
    
    private String businessBean;
    private String businessMethod;
    private Integer totalTimeToProcess;

    // Getters and Setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public String getContainerFactory() {
        return containerFactory;
    }

    public void setContainerFactory(String containerFactory) {
        this.containerFactory = containerFactory;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    public Boolean getAutoStartup() {
        return autoStartup;
    }

    public void setAutoStartup(Boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    public String getClientIdPrefix() {
        return clientIdPrefix;
    }

    public void setClientIdPrefix(String clientIdPrefix) {
        this.clientIdPrefix = clientIdPrefix;
    }

    @Override
    public String toString() {
        return "ListenerData{" +
                "id='" + id + '\'' +
                ", groupId='" + groupId + '\'' +
                ", topics=" + topics +
                ", containerFactory='" + containerFactory + '\'' +
                ", concurrency=" + concurrency +
                ", autoStartup=" + autoStartup +
                ", clientIdPrefix='" + clientIdPrefix + '\'' +
                '}';
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	
	public String getBusinessBean() {
		return businessBean;
	}

	public void setBusinessBean(String businessBean) {
		this.businessBean = businessBean;
	}

	public String getBusinessMethod() {
		return businessMethod;
	}

	public void setBusinessMethod(String businessMethod) {
		this.businessMethod = businessMethod;
	}

	public Integer getTotalTimeToProcess() {
		return totalTimeToProcess;
	}

	public void setTotalTimeToProcess(Integer totalTimeToProcess) {
		this.totalTimeToProcess = totalTimeToProcess;
	}

	public void copy(ListenerData data) throws APIException{
	    if (data == null) {
	        throw new APIException("received null to copy listener data", "", 1);
	    }

	    this.name = data.getName();
	    this.id = data.getId();
	    this.groupId = data.getGroupId();
	    this.topics = data.getTopics();
	    this.containerFactory = data.getContainerFactory();
	    this.concurrency = data.getConcurrency();
	    this.autoStartup = data.getAutoStartup();
	    this.clientIdPrefix = data.getClientIdPrefix();
	    this.businessBean = data.getBusinessBean();
	    this.businessMethod = data.getBusinessMethod();
	    this.totalTimeToProcess = data.getTotalTimeToProcess();
	}
}
