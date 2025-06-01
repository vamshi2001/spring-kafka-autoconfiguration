package com.api.hub.kafka.listener;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Processor {
	
	private Object business;
	private Method methodName;
	private ConsumerRecord<String, String> record;
	AtomicBoolean interreptTask;
	int msgRetryCount;
	boolean failOnReturn;
	boolean failOnException;
	public Processor(Object business, Method methodName, ConsumerRecord<String, String> record, AtomicBoolean interreptTask, int msgRetryCount, boolean failOnReturn, boolean failOnException) {
		this.business = business;
		this.methodName = methodName;
		this.record = record;
		this.interreptTask = interreptTask;
		this.msgRetryCount= msgRetryCount;
		this.failOnReturn = failOnReturn;
		this.failOnException = failOnException;
	}
	
	public RecordHolder call() {
		try {
			if(interreptTask.get()) {
				return null;
			}
			Boolean result = (Boolean) methodName.invoke(business,record.value(), msgRetryCount);
			if(!failOnReturn) {
				return new RecordHolder(true, record);
			}
			if(result!= null && result) {
				return new RecordHolder(true, record);
			}
			return new RecordHolder(false, record);
		}catch (Exception e) {
			if(!failOnException) {
				return new RecordHolder(true, record);
			}
			return new RecordHolder(false, record);
		}
		
	}
}
