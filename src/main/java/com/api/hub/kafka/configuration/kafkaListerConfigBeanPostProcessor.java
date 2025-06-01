package com.api.hub.kafka.configuration;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.stereotype.Component;

import com.api.hub.kafka.common.APIException;
import com.api.hub.kafka.common.AsyncMessageRecoverer;
import com.api.hub.kafka.listener.KafkaListenerBatchTemplet;
import com.api.hub.kafka.pojo.DataHolder;
import com.api.hub.kafka.pojo.ListenerData;
import com.api.hub.kafka.pojo.RecordFilterCache;

import jakarta.annotation.PostConstruct;
import lombok.NonNull;

@Component("kafkaListerConfigBeanPostProcessor")
public class kafkaListerConfigBeanPostProcessor implements BeanPostProcessor {
	
	@Autowired
	ApplicationContext ctx;
	
	@Autowired
	RecordFilterCache filter;
	
	@Autowired
	AsyncMessageRecoverer recoverer;

	private Map<String,ListenerData> listernerData = new HashMap<String, ListenerData>();
	@Autowired
	private ListenerData mainListenerDataInstance;
	
	public void setListenerDataMap(@NonNull Map<String,ListenerData> listernerData) {
		this.listernerData.putAll(listernerData);
	}
	
	@PostConstruct
	public void init() {
		listernerData.putAll(DataHolder.getListenerData());
	}
	
    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
    	ListenerData listener = listernerData.get(beanName);
    	if(listener == null) {
    		return bean;
    	}
    	KafkaListenerBatchTemplet templet = (KafkaListenerBatchTemplet) bean;
    	try {
    		
    		templet.setBean(ctx.getBean(listener.getBusinessBean()));
    		templet.setMethodName(listener.getBusinessMethod());
    		templet.setName(beanName);
    		templet.setTotalTime(listener.getTotalTimeToProcess());
    		templet.setCache(filter);
    		templet.setThreadPool(listener.getNumberOfThreads());
    		templet.setFailOnException(listener.isFailOnException());
    		templet.setFailOnReturn(listener.isFailOnReturn());
    		String recoveryName = listener.getRecovererName();
    		if(recoveryName != null && !recoveryName.isBlank()) {
    			ConsumerRecordRecoverer recvr = (ConsumerRecordRecoverer) ctx.getBean(recoveryName);
    			for(String topic : listener.getTopics()) {
    				recoverer.addRecoverer(topic, recvr);
    			}
    		}
			mainListenerDataInstance.copy(listener);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new FatalBeanException(e.toString());
		}
        return templet;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        
        return bean;
    }
}
