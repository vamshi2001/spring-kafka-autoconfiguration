package com.api.hub.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

public class CustomMessageRecoverer implements ConsumerRecordRecoverer{

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String deadLetterTopic;

    public CustomMessageRecoverer(KafkaTemplate<String, String> kafkaTemplate, String deadLetterTopic) {
        Assert.notNull(kafkaTemplate, "KafkaTemplate must not be null");
        Assert.hasText(deadLetterTopic, "Dead letter topic must not be empty");
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterTopic = deadLetterTopic;
    }

	@Override
	public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        try {
            TopicPartition partition = new TopicPartition(deadLetterTopic, record.partition());

            // Build a message with headers (optional, but helpful for debugging)
            org.springframework.messaging.Message<?> message = MessageBuilder
                    .withPayload(record.value())
                    .setHeader(KafkaHeaders.TOPIC, partition.topic())
                    .setHeader(KafkaHeaders.PARTITION, partition.partition())
                    .setHeader(KafkaHeaders.OFFSET, record.offset())
                    .setHeader("x-exception-class", exception.getClass().getName())
                    .setHeader("x-exception-message", exception.getMessage())
                    .build();

            kafkaTemplate.send(message);
            System.out.println("Message sent to DLT: " + deadLetterTopic + " | Key: " + record.key());
        } catch (Exception e) {
            System.err.println("Failed to publish to DLT: " + e.getMessage());
        }
    }
}
