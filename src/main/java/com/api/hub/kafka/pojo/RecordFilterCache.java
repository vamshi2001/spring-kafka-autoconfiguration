package com.api.hub.kafka.pojo;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class RecordFilterCache {
	
	private int maxAgeMS = 10_000;

	private Map<String,Filter> ListenerFilter = new HashMap<String,Filter>();
	
	public Filter getFilter(String listener){
		return ListenerFilter.getOrDefault(listener, new Filter());
	}
	
	public Map<String,Filter> getALLFilters(){
		return ListenerFilter;
	}
	
	
	public class Filter{
		private volatile Map<String,ConsumerRecord<String, String>> recordFilterMap = new HashMap<String, ConsumerRecord<String,String>>();
		
		public Map<String,ConsumerRecord<String, String>> get(){
			return recordFilterMap;
		}
		public ConsumerRecord<String, String> get(String key) {
			return recordFilterMap.get(key);
		}

		public void setRecordFilterMap(Map<String, ConsumerRecord<String, String>> recordFilterMap) {
			this.recordFilterMap = recordFilterMap;
		}
		
		public synchronized void set(Map<String,ConsumerRecord<String, String>> records) {
			recordFilterMap.putAll(records);
		}
		
		public void remove(String key) {
			recordFilterMap.remove(key);
		}
	}
	
	@Scheduled(initialDelay = 5000, fixedDelay = 10000)
	public void clean() {
		try {
			for(Entry<String,Filter> data : ListenerFilter.entrySet()) {
				 Filter instance = data.getValue();
				 for(Entry<String,ConsumerRecord<String, String>> consumerRecord : instance.get().entrySet()) {
					 long receivedTime = consumerRecord.getValue().timestamp();
					 long currentTime = System.currentTimeMillis();
					 long gap = currentTime - receivedTime;
					 if(gap > maxAgeMS) {
						 instance.remove(consumerRecord.getKey());
					 }
				 }
			}
		}catch (Exception e) {
			log.error("unable to clean record filters" + e.getMessage());
		}
	}
}
