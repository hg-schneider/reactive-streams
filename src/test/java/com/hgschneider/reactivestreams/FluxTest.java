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
import reactor.core.publisher.ConnectableFlux;
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

    @Test
    void flexSubscriperPrettyBackpressure() {
        Flux<Integer> flux = Flux.range(1,10)
            .log()
            .limitRate(3);

            flux.subscribe(
                i -> log.info("Number {}", i)
            );

            log.info("---------------------");
            StepVerifier.create(flux)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .verifyComplete();
        }

    @Test
    public void connectableFlux() throws InterruptedException{
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10) 
            .log()
            .delayElements(Duration.ofMillis(100))
            .publish();
        
        // connectableFlux.connect();

        // log.info("Thread sleeping form 300ms");
        // Thread.sleep(300);

        // connectableFlux.subscribe(
        //     i -> log.info("Sub1 number {}", i)
        // );

        // log.info("Thread sleeping form 200ms");
        // Thread.sleep(200);

        // connectableFlux.subscribe(
        //     i -> log.info("Sub2 number {}", i)
        // );

        StepVerifier
            .create(connectableFlux)
            .then(connectableFlux::connect)
            .thenConsumeWhile(i -> i <= 5)
            .expectNext(6,7,8,9,10)
            .expectComplete()
            .verify();

    }

    @Test
    public void connectableFluxAutoconnect() throws InterruptedException{
        Flux<Integer> fluxAutoConnect = Flux.range(1, 5) 
            .log()
            .delayElements(Duration.ofMillis(100))
            .publish()
            .autoConnect(3);

        
        StepVerifier
            .create(fluxAutoConnect)
            .then(fluxAutoConnect::subscribe)
            .then(fluxAutoConnect::subscribe)
            .expectNext(1,2,3,4,5)
            .expectComplete()
            .verify();

    }
    
    //Next Essentials 15
}
