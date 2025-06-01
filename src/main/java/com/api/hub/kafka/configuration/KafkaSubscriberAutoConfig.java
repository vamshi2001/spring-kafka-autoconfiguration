package com.api.hub.kafka.configuration;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.api.hub.kafka.common.APIException;
import com.api.hub.kafka.common.AsyncMessageRecoverer;
import com.api.hub.kafka.listener.KafkaListenerBatchTemplet;
import com.api.hub.kafka.pojo.DataHolder;
import com.api.hub.kafka.pojo.KafkaListenerContainerConfigProperties;
import com.api.hub.kafka.pojo.ListenerData;
import com.api.hub.kafka.pojo.RecordFilterCache;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class KafkaSubscriberAutoConfig implements BeanDefinitionRegistryPostProcessor{
	
	Map<String,ListenerData> listenerData = new HashMap<String, ListenerData>();
	Map<String,KafkaListenerContainerConfigProperties> containerProperties = new HashMap<String, KafkaListenerContainerConfigProperties>();
	
	private boolean loadListeners() {
		Map<String,ListenerData> data = new HashMap<String, ListenerData>();
		try {
	        ObjectMapper mapper = new ObjectMapper();
	        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        File dir = new File(System.getProperty("kafkaPropPath"));

	        if (!dir.exists() || !dir.isDirectory()) {
	            throw new IllegalArgumentException("Invalid directory: " + System.getProperty("kafkaPropPath"));
	        }

	        File[] jsonFiles = dir.listFiles(new FilenameFilter() {
	            @Override
	            public boolean accept(File dir, String name) {
	                return name.endsWith("-kafka-subscriber.json");
	            }
	        });

	        if (jsonFiles == null) {
	            return false;
	        }
	        
	        for (File file : jsonFiles) {
	            try {
	                ListenerData listenerData = mapper.readValue(file, ListenerData.class);

	                data.put(listenerData.getName(), listenerData);
	               log.debug("Loaded: " + data);
	            } catch (Exception e) {
	                log.error("Failed to load " + file.getName() + ": " + e.getMessage());
	                return false;
	            }
	        }
		}catch (Exception e) {
			log.error("unable to read listeners data");
			return false;
		}
		if(data.size() > 0) {
			DataHolder.putListenerData(data);
			listenerData.putAll(data);
			return true;
		}
		return false;
	}

	private boolean loadContainers() {
		Map<String,KafkaListenerContainerConfigProperties> data = new HashMap<String, KafkaListenerContainerConfigProperties>();
		try {
	        ObjectMapper mapper = new ObjectMapper();
	        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        File dir = new File(System.getProperty("kafkaPropPath"));

	        if (!dir.exists() || !dir.isDirectory()) {
	            throw new IllegalArgumentException("Invalid directory: " + System.getProperty("kafkaPropPath"));
	        }

	        File[] jsonFiles = dir.listFiles(new FilenameFilter() {
	            @Override
	            public boolean accept(File dir, String name) {
	                return name.endsWith("-kafka-container.json");
	            }
	        });

	        if (jsonFiles == null) {
	            return false;
	        }
	        
	        for (File file : jsonFiles) {
	            try {
	            	KafkaListenerContainerConfigProperties containerdata = mapper.readValue(file, KafkaListenerContainerConfigProperties.class);

	                data.put(containerdata.getContainerName(), containerdata);
	               log.debug("Loaded: " + data);
	            } catch (Exception e) {
	                log.error("Failed to load " + file.getName() + ": " + e.getMessage());
	                return false;
	            }
	        }
		}catch (Exception e) {
			log.error("unable to read listeners data");
			return false;
		}
		if(data.size() > 0) {
			DataHolder.putContainerProperties(data);
			containerProperties.putAll(data);
			return true;
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	public void start(BeanDefinitionRegistry ac) throws Exception {
		boolean proceed = true;
		proceed = loadContainers();
		if(!proceed) {
			log.error("failed to read subscriber container properties");
			return;
		}
		proceed =  loadListeners();
		
		if(!proceed) {
			log.error("failed to read subscriber listener properties");
			return;
		}
		try {
			
			for(Entry<String,KafkaListenerContainerConfigProperties> containerData : containerProperties.entrySet()) {
				GenericBeanDefinition beanDefination = new GenericBeanDefinition();
				beanDefination.setBeanClass(ConcurrentKafkaListenerContainerFactory.class);
		        ac.registerBeanDefinition(containerData.getKey(), beanDefination);
			}
			
			for(Entry<String,ListenerData> listener : listenerData.entrySet()) {
				if(!listener.getValue().isEnabled()) {
					continue;
				}
				GenericBeanDefinition beanDefination = new GenericBeanDefinition();
				beanDefination.setBeanClass(KafkaListenerBatchTemplet.class);
				beanDefination.setScope("singleton");
				beanDefination.setDependsOn(listener.getValue().getContainerFactory(), listener.getValue().getRecovererName());
				ac.registerBeanDefinition(listener.getKey(), beanDefination);
			}
			
		}catch (Exception e) {
			throw new APIException("unable to register container/listeners in application context", e.getMessage(), 3);
		}
	}

	@Override
	public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
		try {
			start(registry);
		}catch (Exception e) {
			throw new BeanCreationException("unable to register beans "+e.getMessage());
		}
		
	}
}
