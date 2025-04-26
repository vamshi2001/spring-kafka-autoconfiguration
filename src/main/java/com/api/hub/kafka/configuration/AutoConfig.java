package com.api.hub.kafka.configuration;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import com.api.hub.kafka.pojo.ListenerData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class AutoConfig implements BeanDefinitionRegistryPostProcessor{
	
	
	@Override
	public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
		
		Map<String,Map<String,Object>> map =  loadPublisher();
		if(map == null) {
			log.error("failed to read subscriber listener properties");
			return;
		}
		
		for(String beanName : map.keySet()) {
			ProducerFactory<Integer, String> factory = new DefaultKafkaProducerFactory<Integer, String>(map.get(beanName));
			
			BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(KafkaTemplate.class, () -> new KafkaTemplate<Integer, String>(factory));
			builder.setScope("singleton");
			
			registry.registerBeanDefinition(beanName, builder.getBeanDefinition());
		}
	}
	
	private Map<String,Map<String,Object>> loadPublisher() {
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
	                return name.endsWith("-kafka-publisher.json");
	            }
	        });

	        if (jsonFiles == null) {
	            return null;
	        }
	        Map<String,Map<String,Object>> map = new HashMap<String, Map<String,Object>>();
	        for (File file : jsonFiles) {
	            try {
	            	Map<String,Object> publisherData = mapper.readValue(file, new TypeReference<Map<String,Object>>() {});

	               if(publisherData != null && publisherData.size() > 0) {
	            	   String templetName = file.getName().replace("-kafka-publisher.json", "");
	            	   map.put(templetName, publisherData);
	               }
	               log.debug("Loaded: " + data);
	            } catch (Exception e) {
	                log.error("Failed to load " + file.getName() + ": " + e.getMessage());
	                return null;
	            }
	        }
	        if(map.size() > 0) {
	        	return map;
	        }
		}catch (Exception e) {
			log.error("unable to read listeners data");
			return null;
		}
		return null;
	}

}
