package com.sinch.reactivedemo;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

@Configuration
public class KafkaConfiguration {

  @Bean
  public ReceiverOptions<String, String> receiverOptions(KafkaProperties kafkaProperties) {

    ReceiverOptions<String, String> receiverOptions =
        ReceiverOptions.create(kafkaProperties.buildConsumerProperties());

    return receiverOptions
        .subscription(List.of("test.topic"))
        .consumerProperty(
            ConsumerConfig.GROUP_ID_CONFIG, "test-group-id")
        .withKeyDeserializer(new StringDeserializer())
        .withValueDeserializer(new StringDeserializer());
  }

  @Bean
  public Flux<ReceiverRecord<String, String>> kafkaReceiverFlux(
      ReceiverOptions<String, String> receiverOptions) {
    return KafkaReceiver.create(receiverOptions).receive();
  }
}
