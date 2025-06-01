package com.api.hub.kafka.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import com.api.hub.kafka.common.APIException;
import com.api.hub.kafka.common.AsyncMessageRecoverer;
import com.api.hub.kafka.listener.BatchInterceptorImpl;
import com.api.hub.kafka.pojo.DataHolder;
import com.api.hub.kafka.pojo.KafkaListenerContainerConfigProperties;

import jakarta.annotation.PostConstruct;

@Component
public class KafkaContainerBeanPostProcessor implements BeanPostProcessor {
	
	@Autowired
	GenericApplicationContext ac;
	
	@Autowired
	AsyncMessageRecoverer recoverer;
	
	@PostConstruct
	public void init() {
		containerProperties.putAll(DataHolder.getContainerProperties());
	}

	Map<String,KafkaListenerContainerConfigProperties> containerProperties = new HashMap<String, KafkaListenerContainerConfigProperties>();
	
	
    public void setContainerProperties(Map<String, KafkaListenerContainerConfigProperties> containerProperties) {
		this.containerProperties = containerProperties;
	}

	@SuppressWarnings("unchecked")
	@Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		
		KafkaListenerContainerConfigProperties prop = containerProperties.get(beanName);
    	if(prop == null) {
    		return bean;
    	}
    	try {
			return updateContainer((ConcurrentKafkaListenerContainerFactory<String, String>) bean, prop);
		} catch (APIException e) {
			// TODO Auto-generated catch block
			throw new FatalBeanException(e.toString());
		}
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        
        return bean;
    }
    
    
    private ConcurrentKafkaListenerContainerFactory<String, String> updateContainer(ConcurrentKafkaListenerContainerFactory<String, String> factory, KafkaListenerContainerConfigProperties containerProp) throws APIException {
    	
		try {
			Map<String,Object> consumerProps = containerProp.toMap();
			ConsumerFactory<String,String> consumerFactory = new DefaultKafkaConsumerFactory<String,String>(consumerProps, new StringDeserializer(), new StringDeserializer());
			factory.setConsumerFactory(consumerFactory);
			
	        if(containerProp.isBatchMode()) {
	        	factory.setBatchListener(true);
	        }else {
	        	factory.setBatchListener(false);
			}
	        
	        if(containerProp.isFixedBackOff()) {
	        	FixedBackOff backOff = new FixedBackOff();
	        	backOff.setInterval(0);
	        	backOff.setMaxAttempts(0);
	        	DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
	        	factory.setCommonErrorHandler(errorHandler);
	        }else {
	        	ExponentialBackOff backOff = new ExponentialBackOff();
		        backOff.setInitialInterval(containerProp.getBackOffInitialInterval()); // Start with 1s
		        backOff.setMultiplier(containerProp.getBackOffMultiplier());        // Double the delay every retry
		        backOff.setMaxInterval(containerProp.getBackOffMaxInterval());
		        
		        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
	        	factory.setCommonErrorHandler(errorHandler);
			}
	        factory.setBatchInterceptor(new BatchInterceptorImpl<String, String>());
	        ContainerProperties containerProps = factory.getContainerProperties();

	        containerProps.setAckMode(containerProp.getAckMode());
	        containerProps.setPollTimeout(containerProp.getPollTimeout());
	        containerProps.setIdleEventInterval(containerProp.getIdleEventInterval());
	        containerProps.setMissingTopicsFatal(containerProp.isMissingTopicsFatal());
	        containerProps.setSyncCommits(containerProp.isSyncCommits());
	        containerProps.setIdleBetweenPolls(containerProp.getIdleBetweenPolls());
	        containerProps.setClientId(containerProp.getClientId());
	        containerProps.setLogContainerConfig(containerProp.isLogContainerConfig());
	        containerProps.setDeliveryAttemptHeader(containerProp.isDeliveryAttemptHeader());
		}catch (Exception e) {
			throw new APIException("unable to kafka listener container", "", 2);
		}

        return factory;
    }
}