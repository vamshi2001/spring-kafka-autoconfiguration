package com.api.hub.kafka.listener;

import java.lang.reflect.Method;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Processor {
	
	private Object business;
	private String methodName;
	private ConsumerRecord<String, String> record;
	public Processor(Object business, String methodName, ConsumerRecord<String, String> record) {
		this.business = business;
		this.methodName = methodName;
		this.record = record;
	}
	
	public RecordHolder call() {
		try {
			Method method = business.getClass().getMethod(methodName); // Get the method
			method.invoke(business,record.value());
			return new RecordHolder(true, record);
		}catch (Exception e) {
			return new RecordHolder(false, record);
		}
		
	}
}
