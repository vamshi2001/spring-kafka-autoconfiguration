package com.api.hub.kafka.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import com.api.hub.kafka.pojo.RecordFilterCache;
import com.api.hub.kafka.pojo.RecordFilterCache.Filter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaListenerBatchTemplet implements DisposableBean{
	
	private Object bean;
	private String methodName;
	private int totalTime;
	private String name;
	private RecordFilterCache cache;
	
	public void setTotalTime(int totalTime) {
		this.totalTime = totalTime;
	}
	public void setName(String name) {
		this.name = name;
	}
	public void setCache(RecordFilterCache cache) {
		this.cache = cache;
	}
	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	public void setBean(Object bean) {
		this.bean = bean;
	}
	
	private ExecutorService executor;
	
	public void setThreadPool(int threads) {
		this.executor = Executors.newWorkStealingPool(threads);
	}
	@KafkaListener(topics = "#{ListenerData.topicName}",
			groupId = "#{ListernerData.groupId}",
			containerFactory = "#{ListernerData.containerFactory}",
			autoStartup = "#{ListernerData.autoStartup}",
			concurrency = "#{ListernerData.nConsumers}"
			)
	public void listerner(ConsumerRecords<String, String> records,
			@Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment ack,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long receivedTime,
			@Header(KafkaHeaders.DELIVERY_ATTEMPT) int attempt
			) {
		
				/*
				 * log.debug("processing message - TOPIC - " + topicName +
				 * " RECEIVED_PARTITION " + partitionID + " offset " + offset );
				 */
			Filter filter = cache.getFilter(name);
			boolean isError = false;
			ConsumerRecord<String, String> failedRecord = null;
			Map<String,ConsumerRecord<String, String>> successRecords = new HashMap<String,ConsumerRecord<String, String>>();
			
			
			try {
				List<CompletableFuture<RecordHolder>> futureList = new ArrayList<CompletableFuture<RecordHolder>>();
				for (ConsumerRecord<String, String> record : records) {
					if(attempt > 0) {
						String topicName = record.topic();
						Integer partition = record.partition();
						Long offset = record.offset();
						String key = topicName + "-" + partition.toString() + "-" + offset.toString();
						if(filter.get(key) != null) {
							continue;
						}
					}
			        System.out.println("Received: " + record.value());
			        CompletableFuture<RecordHolder> future = CompletableFuture.supplyAsync(() -> {
			        	return new Processor(bean, methodName, record).call();
			        }, executor);
			        
			        futureList.add(future);
			    }
				
				CompletableFuture<Void> future = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[] {}));
				
				future.get(totalTime, TimeUnit.MILLISECONDS);
				for(CompletableFuture<RecordHolder> task : futureList) {
					
					RecordHolder result = task.get();
					if(!result.isSuccess()) {
						if(failedRecord == null) {
							isError = true;
							failedRecord = result.getRecord();
						}
					} else {
						ConsumerRecord<String, String> successRecord = result.getRecord();
						String topicName = successRecord.topic();
						Integer partition = successRecord.partition();
						Long offset = successRecord.offset();
						String key = topicName + "-" + partition.toString() + "-" + offset.toString();
						successRecords.put(key, successRecord);
					}
					
				}
				
			}catch (Exception e) {
				failedRecord = records.iterator().next();
				isError = true;
			}
			if(isError) {
				filter.set(successRecords);
				throw new BatchListenerFailedException("unable to process record", failedRecord);
			}
		
	}
	
	@Override
	public void destroy() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
