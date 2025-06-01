package com.api.hub.kafka.listener;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
	private Method method;
	private int totalTime;
	private String name;
	private RecordFilterCache cache;
	
	private boolean retryMode = false;
	private boolean failOnReturn;
	private boolean failOnException;
	private ConsumerRecord<String,String> firstFailedRec = null;
	
	public void reset() {
		firstFailedRec = null;
		retryMode = false;
	}
	public void setTotalTime(int totalTime) {
		this.totalTime = totalTime;
	}
	public void setName(String name) {
		this.name = name;
	}
	public  String getName() {
		return name;
	}
	public void setCache(RecordFilterCache cache) {
		this.cache = cache;
	}
	public void setMethodName(String methodName) throws Exception {
		this.methodName = methodName;
		this.method = bean.getClass().getMethod(methodName, String.class, Integer.class);
	}
	public void setBean(Object bean) {
		this.bean = bean;
	}
	public void setFailOnReturn(boolean failOnReturn) {
		this.failOnReturn = failOnReturn;
	}
	public void setFailOnException(boolean failOnException) {
		this.failOnException = failOnException;
	}
	
	private ExecutorService executor;
	
	public void setThreadPool(int threads) {
		this.executor = Executors.newWorkStealingPool(threads);
	}
	
	public Filter getFileter() {
		return cache.getFilter(name);
	}
	int msgRetryCount = 0;
	@KafkaListener(topics = "#{ListenerData.topicName}",
			groupId = "#{ListernerData.groupId}",
			containerFactory = "#{ListernerData.containerFactory}",
			autoStartup = "#{ListernerData.autoStartup}",
			concurrency = "#{ListernerData.nConsumers}"
			)
	public void listerner(List<ConsumerRecord<String,String>> records,
			@Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment ack) {
		
				/*
				 * log.debug("processing message - TOPIC - " + topicName +
				 * " RECEIVED_PARTITION " + partitionID + " offset " + offset );
				 */
			Filter filter = cache.getFilter(name);
			boolean isError = false;
			AtomicBoolean interreptTask = new AtomicBoolean();
			interreptTask.set(false);
			Map<String,ConsumerRecord<String, String>> failedRecords = new HashMap<String,ConsumerRecord<String, String>>();
			
			
			try {
				List<CompletableFuture<RecordHolder>> futureList = new ArrayList<CompletableFuture<RecordHolder>>();
				int temp = findIndex(records, firstFailedRec);
				for (ConsumerRecord<String, String> record : records) {
					
					
					if(retryMode) {
						if(temp-- > 0) {
							continue;
						}
						msgRetryCount = 0;
						if(record.headers().headers("kafka_deliveryAttempt").iterator().hasNext()) {
							org.apache.kafka.common.header.Header header = record.headers().lastHeader("kafka_deliveryAttempt");
							msgRetryCount = ByteBuffer.wrap(header.value()).getInt();
							String topicName = record.topic();
							Integer partition = record.partition();
							Long offset = record.offset();
							String key = topicName + "-" + partition.toString() + "-" + offset.toString();
							if(filter.get(key) != null) {
								continue;
							}
						}else {
							continue;
						}
					}
						
					
			        System.out.println("Received: " + record.value());
			        CompletableFuture<RecordHolder> future = CompletableFuture.supplyAsync(() -> {
			        	return new Processor(bean, method, record, interreptTask, msgRetryCount, failOnReturn, failOnException).call();
			        }, executor);
			        
			        futureList.add(future);
			    }
				if(futureList.size() < 1) {
					return;
				}
				CompletableFuture<Void> future = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[] {}));
				
				if(totalTime > 0) {
					future.get(totalTime, TimeUnit.MILLISECONDS);
				}else {
					future.get();
				}
				ConsumerRecord<String,String> failedrecTemp = null;
				for(CompletableFuture<RecordHolder> task : futureList) {
					
					RecordHolder result = task.get();
					if(!result.isSuccess()) {
						ConsumerRecord<String,String> failedRecord = result.getRecord();
						String topicName = failedRecord.topic();
						Integer partition = failedRecord.partition();
						Long offset = failedRecord.offset();
						String key = topicName + "-" + partition.toString() + "-" + offset.toString();
						failedRecords.put(key, failedRecord);
						isError = true;
						if(failedrecTemp == null) {
							failedrecTemp = result.getRecord();
						}
					}
					
				}
				firstFailedRec = failedrecTemp;
			}catch (Exception e) {

				interreptTask.set(true);
				isError = true;
				if(firstFailedRec == null) {
					firstFailedRec = records.get(0);
				}
				records.forEach((failedRecord) -> {
					String topicName = failedRecord.topic();
					Integer partition = failedRecord.partition();
					Long offset = failedRecord.offset();
					String key = topicName + "-" + partition.toString() + "-" + offset.toString();
					failedRecords.put(key, failedRecord);
				});
			}
			if(isError) {
				retryMode = true;
				filter.set(failedRecords);
				throw new BatchListenerFailedException("unable to process record", firstFailedRec);
			}
			firstFailedRec = null;
			retryMode = false;
			ack.acknowledge();
	}
	
	@Override
	public void destroy() throws Exception {
		// TODO Auto-generated method stub
		
	}
	private int findIndex(List<ConsumerRecord<String,String>> data, ConsumerRecord<?, ?> record) {
		if(record == null) {
			return -1;
		}
		
		int i = 0;
		Iterator<?> iterator = data.iterator();
		while(iterator.hasNext()) {
			ConsumerRecord<?, ?> candidate = (ConsumerRecord<?, ?>) iterator.next();
			if(candidate.topic().equals(record.topic()) && candidate.partition() == record.partition() && candidate.offset() == record.offset()) {
				break;
			}
			i++;
		}
		
		return i;
	}

}
