package com.hgschneider.reactivestreams;


import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

// DevDojo Academy
// Project Reactor Essentials
// https://www.youtube.com/watch?v=lCTUOERTXyw&list=PL0Un1HNdB4jFCsHsQg2HOfO03XfECuMiw
@Slf4j
class MonoTest {
    private static final String MESSAGE = "Hello world!";

    @Test
    void monoSubscriber() {
        Mono<String> mono = Mono.just(MESSAGE)
            .log();
        
        mono.subscribe();
        log.info("---------------------");
        StepVerifier.create(mono)
            .expectNext(MESSAGE)
            .verifyComplete();
    }

    @Test
    void monoSubscriberConsumer() {
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
    void monoSubscriberError() {
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
    void monoSubscriberConsumerComplete() {
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
    void monoSubscriberConsumerSubscription() {
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
    void monoDoOnMethod() {
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

    @Test
    void monoDoOnError() {
        Mono<String> mono = Mono.<String>error(new IllegalArgumentException("Something Illegal."))
            .doOnError(e -> log.error("Error!"))
            .log();

        StepVerifier.create(mono)
            .expectError(IllegalArgumentException.class)
            .verify();
    }

    @Test
    void monoDoOnErrorResume() {
        Mono<String> mono = Mono.<String>error(new IllegalArgumentException("Something Illegal."))
            .doOnError(e -> log.error("Error!"))
            .onErrorResume(s -> {
                log.info("Inside on error resume.");
                return(Mono.just("resuming"));
            })
            .log();

        StepVerifier.create(mono)
            .expectNext("resuming")
            .verifyComplete();
    }

    @Test
    void monoDoOnErrorReturn() {
        Mono<String> mono = Mono.<String>error(new IllegalArgumentException("Something Illegal."))
            .onErrorReturn("On error return")
            .onErrorResume(s -> {
                log.info("Inside on error resume.");
                return(Mono.just("resuming"));
            })
            .doOnError(e -> log.error("Error!"))
            .log();

        StepVerifier.create(mono)
            .expectNext("On error return")
            .verifyComplete();
    }
}
