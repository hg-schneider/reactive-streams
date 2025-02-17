package com.hgschneider.reactivestreams;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@Slf4j
class FluxTest {

    @Test
     void fluxSubscriber() {
        Flux<String> fluxString = Flux.just("Jo", "Jack", "William", "Averell")
            .log();

            StepVerifier.create(fluxString)
            .expectNext("Jo", "Jack", "William", "Averell")
            .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbers() {
       Flux<Integer> flux = Flux.range(1, 5)
           .log();

        flux.subscribe(i -> log.debug("{}", i));

        log.info("---------------------");
        StepVerifier.create(flux)
           .expectNext(1,2,3,4,5)
           .verifyComplete();
   }

    @Test
    void fluxSubscriberList() {
       Flux<Integer> flux = Flux.fromIterable(List.of(1, 2,3,4,5,6))
           .log();

        flux.subscribe(i -> log.debug("{}", i));
        
        log.info("---------------------");
        StepVerifier.create(flux)
           .expectNext(1,2,3,4,5,6)
           .verifyComplete();
   }

    @Test
    void fluxSubscriberNumbersError() {
        Flux<Integer> flux = Flux.range(1, 5)
          .log()
          .map(i -> { 
            if(i == 4) { 
                throw new IndexOutOfBoundsException("Bad index."); 
            }
            return i;
          });

       flux.subscribe(
            i -> log.debug("{}", i),
            e -> log.error("The Exception", e),
            () -> log.info("done"),
            s -> s.request(3)
        );

       log.info("---------------------");
       StepVerifier.create(flux)
          .expectNext(1,2,3)
          .expectError(IndexOutOfBoundsException.class)
          .verify();
  }

    @Test
    void fluxSubscriberNumbersUglyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
            .log();

        flux.subscribe(new Subscriber<Integer>() {
            private int count = 0;
            private Subscription subscription;
            private final int requestCount=2;

            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                s.request(2);
                
            }
            @Override
            public void onNext(Integer t) {
                count++;
                if(count >= requestCount) {
                    count = 0;
                    subscription.request(requestCount);
                }
            }

            @Override
            public void onError(Throwable t) {
                
            }
            @Override
            public void onComplete() {
                
            }
        });

        log.info("---------------------");
        StepVerifier.create(flux)
            .expectNext(1,2,3,4,5,6,7,8,9,10)
            .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbersNotSoUglyBackpressure() {
    Flux<Integer> flux = Flux.range(1, 10)
        .log();

        flux.subscribe(new BaseSubscriber<Integer>() {
            private int count = 0;
            private final int requestCount=2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(2);
            }


            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if(count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        log.info("---------------------");
        StepVerifier.create(flux)
            .expectNext(1,2,3,4,5,6,7,8,9,10)
            .verifyComplete();
    }

    @Test
    void fluxSubscriberIntervalOne() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
            .take(10)
            .log();

        interval.subscribe(
            i -> log.debug("Number {}", i)
        );

        Thread.sleep(3000);
    }

    @Test
    void fluxSubscriberIntervalTwo() throws InterruptedException {
        Supplier<Flux<Long>> createInterval = () -> Flux.interval(Duration.ofDays(1)).log();

        StepVerifier.withVirtualTime(createInterval)
            .expectSubscription()
            .expectNoEvent(Duration.ofHours(24))
            .thenAwait(Duration.ofDays(1))
            .expectNext(0L)
            .thenAwait(Duration.ofDays(1))
            .expectNext(1L)
            .thenCancel()
            .verify();
    }
    //Next Essentials 12 
}
