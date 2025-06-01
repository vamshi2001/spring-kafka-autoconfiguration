package com.api.hub.kafka.listener;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.listener.BatchInterceptor;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;

public class BatchInterceptorImpl<K,V> implements BatchInterceptor<K, V> {

	@Override
	public ConsumerRecords<K, V> intercept(ConsumerRecords<K, V> records, Consumer<K, V> consumer) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void failure(ConsumerRecords<K, V> records, Exception exception, Consumer<K, V> consumer) {
		Throwable throwableException = null;
		if(exception instanceof ListenerExecutionFailedException) {
			ListenerExecutionFailedException ex = (ListenerExecutionFailedException) exception;
			throwableException = ex.getCause();
		}
		
		if(exception instanceof BatchListenerFailedException || throwableException!= null && throwableException instanceof BatchListenerFailedException) {
			BatchListenerFailedException exp = (BatchListenerFailedException) (throwableException != null ? throwableException : exception);
			int index = findIndex(records, exp.getRecord());
			
			for(ConsumerRecord<K, V> rec : records) {
				if(index-- > 0) {
					// these are successfull recs in sequence
				}else {
					// from here we found first failed rec in sequence
					
					Headers headers = rec.headers();
					int attempt = 1;
					if(headers.lastHeader(KafkaHeaders.DELIVERY_ATTEMPT) != null) {
						byte[] value = headers.lastHeader(KafkaHeaders.DELIVERY_ATTEMPT).value();
						attempt = ByteBuffer.wrap(value).getInt() + 1;
						
						headers.remove(KafkaHeaders.DELIVERY_ATTEMPT);
					}
					
					headers.add(new RecordHeader(KafkaHeaders.DELIVERY_ATTEMPT, ByteBuffer.allocate(4).putInt(attempt).array()));
				}
			}
		}
	}

	private int findIndex(ConsumerRecords<K, V> data, ConsumerRecord<?, ?> record) {
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
