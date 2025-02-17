package com.hgschneider.reactivestreams;

import javax.management.RuntimeErrorException;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

// DevDojo Academy
// Project Reactor Essentials
// https://youtu.be/lCTUOERTXyw?si=s0HNtjhp3NabxUuJ
@Slf4j
public class MonoTest {
    private static final String MESSAGE = "Hello world!";

    @Test
    public void monoSubscriber() {
        Mono<String> mono = Mono.just(MESSAGE)
            .log();
        
        mono.subscribe();
        log.info("---------------------");
        StepVerifier.create(mono)
            .expectNext(MESSAGE)
            .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumer() {
        Mono<String> mono = Mono.just(MESSAGE)
            .log();

        mono.subscribe(
            s -> {log.info("Value: {}", s);
        });

        log.info("---------------------");
        StepVerifier.create(mono)
            .expectNext(MESSAGE)
            .verifyComplete();
    }

    @Test
    public void monoSubscriberError() {
        Mono<String> mono = Mono.just(MESSAGE)
            .map(s -> { throw new RuntimeException("Testing error!"); });
            
        mono.subscribe(
            s -> { log.info("Value: {}", s); },
            t -> { log.error("Error happend!", t);}
        );

        log.info("---------------------");
        StepVerifier.create(mono)
            .expectError(RuntimeException.class)
            .verify();
    }

    @Test
    public void monoSubscriberConsumerComplete() {
        Mono<String> mono = Mono.just(MESSAGE)
            .log()
            .map(s -> s.toUpperCase());

        mono.subscribe(
            s -> { log.info("Value: {}", s);},
            t -> { log.error("Error!", t); },
            () -> {log.info("Finished!");}
        );
        log.info("---------------------");
        StepVerifier.create(mono)
            .expectNext(MESSAGE.toUpperCase())
            .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerSubscription() {
        Mono<String> mono = Mono.just(MESSAGE)
            .log()
            .map(s -> s.toUpperCase());

        mono.subscribe(
            s -> { log.info("Value: {}", s);},
            t -> { log.error("Error!", t); },
            () -> {log.info("Finished!");},
            s -> s.request(5)
        );
        log.info("---------------------");
        StepVerifier.create(mono)
            .expectNext(MESSAGE.toUpperCase())
            .verifyComplete();
    }

    @Test
    public void monoDoOnMethod() {
        Mono<String> mono = Mono.just(MESSAGE)
            .log()
            .map(s -> s.toUpperCase())
            .doOnSubscribe(s -> log.info("Subscribed"))
            .doOnRequest(l -> log.info("Request received, starting doing something"))
            .doOnNext(s -> log.info("Value us here. Executing doOnNext {}",s))
            .flatMap(s -> Mono.<String>empty())
            .doOnNext(s -> log.info("Value us here. Executing doOnNext {}",s))
            .doOnSuccess(s -> log.info("doOnSuccess executed {}", s));

        mono.subscribe(
            s -> { log.info("Value: {}", s);},
            t -> { log.error("Error!", t); },
            () -> {log.info("Finished!");}
        );
        log.info("---------------------");
        StepVerifier.create(mono)
            .expectNextCount(0)
            .verifyComplete();
    }
    //Next Chapter 07
}
