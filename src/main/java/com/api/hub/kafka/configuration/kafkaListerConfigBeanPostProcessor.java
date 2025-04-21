package com.api.hub.kafka.configuration;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import com.api.hub.kafka.common.APIException;
import com.api.hub.kafka.pojo.ListenerData;

import lombok.NonNull;

@Component("kafkaListerConfigBeanPostProcessor")
public class kafkaListerConfigBeanPostProcessor implements BeanPostProcessor {

	private Map<String,ListenerData> listernerData = new HashMap<String, ListenerData>();
	@Autowired
	private ListenerData mainListenerDataInstance;
	
	public void setListenerDataMap(@NonNull Map<String,ListenerData> listernerData) {
		this.listernerData.putAll(listernerData);
	}
	
    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
    	ListenerData listener = listernerData.get(beanName);
    	if(listener == null) {
    		return bean;
    	}
    	try {
			mainListenerDataInstance.copy(listener);
		} catch (APIException e) {
			// TODO Auto-generated catch block
			throw new FatalBeanException(e.toString());
		}
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        
        return bean;
    }
}
