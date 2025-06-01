package com.api.hub.kafka.pojo;

import java.util.HashMap;
import java.util.Map;

public class DataHolder {

	private static Map<String, ListenerData> listenerData = new HashMap<String, ListenerData>();
	private static Map<String, KafkaListenerContainerConfigProperties> containerproperties = new HashMap<String, KafkaListenerContainerConfigProperties>();
	
	public static void putListenerData(Map<String,ListenerData> data) {
		listenerData.putAll(data);
	}
	
	public static void putContainerProperties(Map<String, KafkaListenerContainerConfigProperties> data) {
		containerproperties.putAll(data);
	}
	
	public static Map<String, ListenerData> getListenerData(){
		return listenerData;
	}
	
	public static Map<String, KafkaListenerContainerConfigProperties> getContainerProperties(){
		return containerproperties;
	}
}
