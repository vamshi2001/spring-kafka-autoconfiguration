package com.api.hub.kafka.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component("AsyncMessageRecoverer")
@Slf4j
public class AsyncMessageRecoverer implements ConsumerRecordRecoverer{

	private Map<String, ConsumerRecordRecoverer> map = new HashMap<String, ConsumerRecordRecoverer>();
	
	public void addRecoverer(String topicName,ConsumerRecordRecoverer recoverer ) {
		map.put(topicName, recoverer);
	}
	@Override
	public void accept(ConsumerRecord<?, ?> record, Exception exception) {
		try {
			
			CompletableFuture.runAsync(() -> {
				ConsumerRecordRecoverer recoverer = map.get(record.topic());
				if(recoverer != null) {
					recoverer.accept(record, exception);
				}else {
					log.info("recovered msg" + record.value());
				}
			});
		}catch (Exception e) {
			System.err.println("Failed to recover record: " + e.getMessage());
		}
	}
}
