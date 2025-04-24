package com.api.hub.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RecordHolder {
	private boolean success;
	private ConsumerRecord<String, String> record;
	public RecordHolder(boolean success, ConsumerRecord<String, String> record) {
		super();
		this.success = success;
		this.record = record;
	}
	public boolean isSuccess() {
		return success;
	}
	public ConsumerRecord<String, String> getRecord() {
		return record;
	}
	
}
