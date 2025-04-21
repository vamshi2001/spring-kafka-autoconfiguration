package com.api.hub.kafka.configuration;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.stereotype.Component;

import com.api.hub.kafka.common.APIException;
import com.api.hub.kafka.listener.KafkaListenerBatchTemplet;
import com.api.hub.kafka.pojo.KafkaListenerContainerConfigProperties;
import com.api.hub.kafka.pojo.ListenerData;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class KafkaSubscriberAutoConfig {
	
	@Autowired
	kafkaListerConfigBeanPostProcessor processor;
	
	@Autowired
	ConfigurableListableBeanFactory ctx;
	
	@Autowired
	GenericApplicationContext ac;
	
	@Autowired
	KafkaContainerBeanPostProcessor containerProcessor;
	
	@Autowired
	ListenerData listenerData;
	
	@Value("${directoryPathToScan}")
	String directoryPath;
	
	Map<String,ListenerData> listenerdata;
	Map<String,KafkaListenerContainerConfigProperties> containerProperties;
	
	private boolean loadListeners() {
		Map<String,ListenerData> data = new HashMap<String, ListenerData>();
		try {
	        ObjectMapper mapper = new ObjectMapper();
	        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        File dir = new File(directoryPath);

	        if (!dir.exists() || !dir.isDirectory()) {
	            throw new IllegalArgumentException("Invalid directory: " + directoryPath);
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
			processor.setListenerDataMap(data);
			listenerdata.putAll(data);
			return true;
		}
		return false;
	}

	private boolean loadContainers() {
		Map<String,KafkaListenerContainerConfigProperties> data = new HashMap<String, KafkaListenerContainerConfigProperties>();
		try {
	        ObjectMapper mapper = new ObjectMapper();
	        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        File dir = new File(directoryPath);

	        if (!dir.exists() || !dir.isDirectory()) {
	            throw new IllegalArgumentException("Invalid directory: " + directoryPath);
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
			containerProperties.putAll(data);
			containerProcessor.setContainerProperties(data);
			return true;
		}
		return false;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void start() throws Exception {
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
		        ac.registerBean(containerData.getKey(), ConcurrentKafkaListenerContainerFactory.class);
			}
			for(Entry<String,ListenerData> listener : listenerdata.entrySet()) {
				
				ac.registerBean(listener.getKey(), KafkaListenerBatchTemplet.class, (BeanDefinition bd) ->{
					bd.setScope("singleton");
					bd.setDependsOn(listener.getValue().getContainerFactory());
				});
			}
			
			ac.refresh();
			
		}catch (Exception e) {
			throw new APIException("unable to register container/listeners in application context", e.getMessage(), 3);
		}
	}
}
