package com.sinch.reactivedemo;

import ch.qos.logback.core.joran.conditional.ThenAction;
import java.util.HashMap;
import org.springframework.http.HttpHeaders;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
@Component
@AllArgsConstructor
public class KafkaConsumer {

  private final Flux<ReceiverRecord<String, String>> kafkaReceiverFlux;
  private final WebClient webClient;

  @EventListener(ApplicationReadyEvent.class)
  public void consume() {
    kafkaReceiverFlux
        .map(record -> record.value())
        .doOnNext(url -> log.info("Received URL: {}", url))
        .flatMap(url -> headRequestToUrl(url))
        .filter(headers -> headers.getFirst(HttpHeaders.CONTENT_TYPE).contains("image/"))
        .doOnNext(headers -> log.info("Image URL headers: {}", headers))
        .doOnError(error -> log.error("Error during URL fetch", error))
        .subscribe();
  }

  public Mono<HttpHeaders> headRequestToUrl(String url) {
    return webClient
        .head()
        .uri(url)
        .retrieve()
        .toBodilessEntity()
        .map(response -> response.getHeaders());
  }

}
