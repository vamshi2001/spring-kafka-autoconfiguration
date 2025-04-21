package com.api.hub.kafka.listener;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaListenerBatchTemplet implements DisposableBean{
	
	private ExecutorService executor = Executors.newWorkStealingPool(0);
	
	public void setThreadPool(ExecutorService executor) {
		this.executor = executor;
	}
	@KafkaListener(topics = "#{ListenerData.topicName}",
			groupId = "#{ListernerData.groupId}",
			containerFactory = "#{ListernerData.containerFactory}",
			autoStartup = "#{ListernerData.autoStartup}",
			concurrency = "#{ListernerData.nConsumers}"
			)
	public void listerner(@Payload List<String> data,
			@Header(KafkaHeaders.RECEIVED_PARTITION) int partitionID,
			@Header(KafkaHeaders.OFFSET) int offset,
			@Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment ack,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long receivedTime,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topicName,
			@Header(KafkaHeaders.DELIVERY_ATTEMPT) int attempt
			) {
		
			log.debug("processing message - TOPIC - " + topicName + " RECEIVED_PARTITION " +
					partitionID + " offset " + offset );
			try {
				
			}catch (Exception e) {
				Duration duration = Duration.ofSeconds(5);
				ack.nack(duration);
			}
		
	}
	
	
	@Override
	public void destroy() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
