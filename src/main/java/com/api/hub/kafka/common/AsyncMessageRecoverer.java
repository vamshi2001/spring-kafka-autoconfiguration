package com.api.hub.kafka.common;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import com.api.hub.kafka.pojo.RecordFilterCache;
import com.api.hub.kafka.pojo.RecordFilterCache.Filter;

@Component("AsyncMessageRecoverer")
public class AsyncMessageRecoverer implements ConsumerRecordRecoverer{

	private Object bean;
	private String methodName;
	
	private String deafaultTopicName;
	private boolean publishToTopic;
	private KafkaTemplate<String, String> kafkaTemplate = null;
	
	private RecordFilterCache cache;
	
	public void setCache(RecordFilterCache cache) {
		this.cache = cache;
	}
	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	public void setBean(Object bean) {
		this.bean = bean;
	}

	public void setDeafaultTopicName(String deafaultTopicName) {
		this.deafaultTopicName = deafaultTopicName;
	}
	public void setPublishToTopic(boolean publishToTopic) {
		this.publishToTopic = publishToTopic;
	}
	public void setKafkaTemplate(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}
	@Override
	public void accept(ConsumerRecord<?, ?> record, Exception exception) {
		try {
			boolean skipRecord = false;
			Map<String,Filter> allFilters = cache.getALLFilters();{
				for(Filter filter : allFilters.values()) {
					String topicName = record.topic();
					Integer partition = record.partition();
					Long offset = record.offset();
					String key = topicName + "-" + partition.toString() + "-" + offset.toString();
					if(filter.get(key) != null) {
						skipRecord = true;
						break;
					}
				}
			}
			
			if(!skipRecord) {
				CompletableFuture.runAsync(() -> {
					try {
						if(publishToTopic) {
							publishToTopic(record, exception);
						}else {
							Method method = bean.getClass().getMethod(methodName); // Get the method
							method.invoke(bean,record, exception);
						}
					}catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
		}catch (Exception e) {
			System.err.println("Failed to recover record: " + e.getMessage());
		}
	}
	
	public void publishToTopic(ConsumerRecord<?, ?> record, Exception exception) {
        try {
            TopicPartition partition = new TopicPartition(deafaultTopicName, record.partition());

            // Build a message with headers (optional, but helpful for debugging)
            org.springframework.messaging.Message<?> message = MessageBuilder
                    .withPayload(record.value())
                    .setHeader(KafkaHeaders.TOPIC, partition.topic())
                    .setHeader(KafkaHeaders.PARTITION, partition.partition())
                    .setHeader(KafkaHeaders.OFFSET, record.offset())
                    .setHeader("x-exception-class", exception.getClass().getName())
                    .setHeader("x-exception-message", exception.getMessage())
                    .build();

            kafkaTemplate.send(message);
            System.out.println("Message sent to DLT: " + deafaultTopicName + " | Key: " + record.key());
        } catch (Exception e) {
            System.err.println("Failed to publish to DLT: " + e.getMessage());
        }
    }
}
