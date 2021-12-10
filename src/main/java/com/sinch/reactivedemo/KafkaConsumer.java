package com.sinch.reactivedemo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
@Component
public class KafkaConsumer {

  private final Flux<ReceiverRecord<String, String>> kafkaReceiverFlux;

  public KafkaConsumer(
      Flux<ReceiverRecord<String, String>> kafkaReceiverFlux) {
    this.kafkaReceiverFlux = kafkaReceiverFlux;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void consume() {
    kafkaReceiverFlux
        .doOnNext(url -> log.info("Received URL: {}", url))
        .flatMap(url -> downloadUrl())
        .filter()
        .map(media -> media.size())

        .doOnError()
        .subscribe();
  }

}
