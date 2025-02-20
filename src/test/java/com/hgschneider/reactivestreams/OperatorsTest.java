package com.hgschneider.reactivestreams;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

// DevDojo Academy
// Project Reactor Essentials
// https://www.youtube.com/watch?v=lCTUOERTXyw&list=PL0Un1HNdB4jFCsHsQg2HOfO03XfECuMiw

// BlockHound

@Slf4j
class OperatorsTest {
    
    @BeforeAll
    static void setUp() {
        BlockHound.install();
    }

    @Test 
    void blockHoundworks() {
        try {
            FutureTask<?> task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });
            Schedulers.parallel().schedule(task);

            task.get(10, TimeUnit.SECONDS);
            Assertions.fail("should fail");
        } catch(Exception e)  {
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }
    }

    // Project Reactor Essentials 15
    @Test
    void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1,4)
        .map(i -> {
           log.info("Map 1 - number {} on thread {}", i , Thread.currentThread().getName()); 
           return i;
        })
        .subscribeOn(Schedulers.boundedElastic())
        .map(i -> {
            log.info("Map 2 - number {} on thread {}", i , Thread.currentThread().getName()); 
            return i;
         });

         StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    @Test
    void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1,4)
        .map(i -> {
           log.info("Map 1 - number {} on thread {}", i , Thread.currentThread().getName()); 
           return i;
        })
        .publishOn(Schedulers.boundedElastic())
        .map(i -> {
            log.info("Map 2 - number {} on thread {}", i , Thread.currentThread().getName()); 
            return i;
         });

         StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    //Project Reactor Essentials 16
    @Test
    void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1,4)
        .subscribeOn(Schedulers.boundedElastic())
        .map(i -> {
           log.info("Map 1 - number {} on thread {}", i , Thread.currentThread().getName()); 
           return i;
        })
        .subscribeOn(Schedulers.single())
        .map(i -> {
            log.info("Map 2 - number {} on thread {}", i , Thread.currentThread().getName()); 
            return i;
         });

         StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    @Test
    void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1,4)
        .publishOn(Schedulers.single())
        .map(i -> {
           log.info("Map 1 - number {} on thread {}", i , Thread.currentThread().getName()); 
           return i;
        })
        .publishOn(Schedulers.boundedElastic())
        .map(i -> {
            log.info("Map 2 - number {} on thread {}", i , Thread.currentThread().getName()); 
            return i;
         });

         StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    @Test
    void publishAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1,4)
        .publishOn(Schedulers.single())
        .map(i -> {
           log.info("Map 1 - number {} on thread {}", i , Thread.currentThread().getName()); 
           return i;
        })
        .subscribeOn(Schedulers.boundedElastic())
        .map(i -> {
            log.info("Map 2 - number {} on thread {}", i , Thread.currentThread().getName()); 
            return i;
         });

         StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    @Test
    void subscribeAudPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1,4)
        .subscribeOn(Schedulers.single())
        .map(i -> {
           log.info("Map 1 - number {} on thread {}", i , Thread.currentThread().getName()); 
           return i;
        })
        .publishOn(Schedulers.boundedElastic())
        .map(i -> {
            log.info("Map 2 - number {} on thread {}", i , Thread.currentThread().getName()); 
            return i;
         });

         StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
    }

    //Project Reactor Essentials 17
    @Test
    void subscribeOnIo() {
        Mono<List<String>> list = Mono.fromCallable(() -> {
                URL url = OperatorsTest.class.getClassLoader().getResource("text-file.txt");
                return Files.readAllLines(Path.of(url.toURI()));
            })
            .log()
            .subscribeOn(Schedulers.boundedElastic()
        );

        //list.subscribe(s -> log.info("{}", s));

        StepVerifier.create(list)
            .expectSubscription()
            .thenConsumeWhile(l -> {
                Assertions.assertFalse(l.isEmpty());
                log.info("Size {}", l.size());
                return true;
            })
            .verifyComplete();
    }

    //Project Reactor Essentials 18
    @Test
    void switchIfEmptyOperator() {
        Flux<Object> flux = Flux.empty()
            .switchIfEmpty(Flux.just("not empty anymore"))
            .log();


        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext("not empty anymore")
            .expectComplete()
            .verify();
    }

    @Test
    void deferOperator() throws Exception {
        Mono<Long> just = Mono.just(System.currentTimeMillis());
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        defer.subscribe(l -> log.info("time 1 {}: " + l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time 2 {}: " + l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time 3 {}: " + l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time 4 {}: " + l));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);
    }

    //Project Reactor Essentials 19
    @Test
    void concatOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> fluxConcatenated = Flux.concat(flux1, flux2).log();

        StepVerifier.create(fluxConcatenated)
            .expectSubscription()
            .expectNext("a", "b", "c", "d")
            .expectComplete()
            .verify();
    }

    @Test
    void concatWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> fluxConcatenated = flux1.concatWith(flux2).log();
        
        StepVerifier.create(fluxConcatenated)
            .expectSubscription()
            .expectNext("a", "b", "c", "d")
            .expectComplete()
            .verify();
    }

    @Test
    void combineLatestOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> fluxCombineLatest = Flux.combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase() )
            .log();
        
        StepVerifier.create(fluxCombineLatest)
            .expectSubscription()
            .expectNext("BC", "BD")
            .expectComplete()
            .verify();
    }

    //Project Reactor Essentials 20
    @Test
    void mergeOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergedFlux = Flux.merge(flux1, flux2).log();

        StepVerifier.create(mergedFlux)
            .expectSubscription()
            .expectNext("a", "b", "c", "d")
            .expectComplete()
            .verify();
    }

    @Test
    void mergeWithOperator() throws Exception {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(100));
        Flux<String> flux2 = Flux.just("c", "d").delayElements(Duration.ofMillis(110));

        Flux<String> mergedFlux = flux1.mergeWith(flux2).log();

        Thread.sleep(1000);

        StepVerifier.create(mergedFlux)
            .expectSubscription()
            .expectNext("a", "c", "b", "d")
            .expectComplete()
            .verify();
    }

    //Project Reactor Essentials 21
    @Test
    void mergeSequentialOperator() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(100));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergedFlux = Flux.mergeSequential(flux1, flux2, flux1)
            .delayElements(Duration.ofMillis(200))
            .log();

        StepVerifier.create(mergedFlux)
            .expectSubscription()
            .expectNext("a", "b", "c", "d", "a", "b")
            .expectComplete()
            .verify();
    }

    @Test
    void concatOperatorError() {
        Flux<String> flux1 = Flux.just("a", "b")
            .map(s-> {
                if(s.equals("b")) {
                    throw new IllegalArgumentException();
                }
                return s;
            } );
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> fluxConcatenated = Flux.concatDelayError(flux1, flux2).log();

        StepVerifier.create(fluxConcatenated)
            .expectSubscription()
            .expectNext("a", "c", "d")
            .expectError(IllegalArgumentException.class)
            .verify();
    }

    @Test
    void mergeDelayErrorlOperator() {
        Flux<String> flux1 = Flux.just("a", "b")
            .map(s -> {
                if(s.equals("b")) {
                    throw new IllegalArgumentException();
                }
                return s;
            })
            .doOnError(t -> log.error("Do something with this"));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergedFlux = Flux.mergeDelayError(1, flux1, flux2, flux1)
            .log();

        StepVerifier.create(mergedFlux)
            .expectSubscription()
            .expectNext("a", "c", "d", "a")
            .expectError()
            .verify();
    }
    //Project Reactor Essentials 22
    @Test
    void flatMapOperator() throws Exception {
        Function<String, Flux<String>> myFunc = (String name) -> {
            return name.equals("A") ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(100)) : Flux.just("nameB1", "nameB2");
        };

        Flux<String> flux = Flux.just("a","b");
        Flux<String> flatFlux = flux
            .map(String::toUpperCase)
            .flatMap(myFunc)
            .log();

            flatFlux.subscribe(s -> log.info(s));

        Thread.sleep(1000);
        StepVerifier
            .create(flatFlux)
            .expectSubscription()
            .expectNext("nameB1", "nameB2", "nameA1", "nameA2")
            .verifyComplete();

    }

    @Test
    void flatMapSequentialOperator() throws Exception {
        Function<String, Flux<String>> myFunc = (String name) -> {
            return name.equals("A") ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(100)) : Flux.just("nameB1", "nameB2");
        };

        Flux<String> flux = Flux.just("a","b");
        Flux<String> flatFlux = flux
            .map(String::toUpperCase)
            .flatMapSequential(myFunc)
            .log();

            flatFlux.subscribe(s -> log.info(s));

        Thread.sleep(1000);
        StepVerifier
            .create(flatFlux)
            .expectSubscription()
            .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
            .verifyComplete();

    }

    //Project Reactor Essentials 23
    public record Anime(String title, String studio, int epsode) {}

    @Test
    void zipOperator() {
        Flux<String> titleFlux = Flux.just("Grand Blue", "Baki");
        Flux<String> studioFlux = Flux.just("Zero-G", "TMS Entertainment");
        Flux<Integer> episodesFlux = Flux.just(12,24);


        Flux<Anime> animeFlux = Flux.zip(titleFlux, studioFlux, episodesFlux)
            .flatMap(t3 -> Flux.just(new Anime(t3.getT1(), t3.getT2(), t3.getT3())));

        StepVerifier
            .create(animeFlux)
            .expectSubscription()
            .expectNext(
                new Anime("Grand Blue", "Zero-G", 12),
                new Anime("Baki", "TMS Entertainment", 24)
            )
            .verifyComplete();
    }

    @Test
    void zipOperatorWith() {
        Flux<String> titleFlux = Flux.just("Grand Blue", "Baki");
        Flux<String> studioFlux = Flux.just("Zero-G", "TMS Entertainment");
        Flux<Integer> episodesFlux = Flux.just(12,24);


        Flux<Anime> animeFlux = titleFlux.zipWith(studioFlux)
            .flatMap(t2 -> Flux.just(new Anime(t2.getT1(), t2.getT2(), 0)));

        animeFlux.subscribe(a -> log.info(a.toString()));

        StepVerifier
            .create(animeFlux)
            .expectSubscription()
            .expectNext(
                new Anime("Grand Blue", "Zero-G", 0),
                new Anime("Baki", "TMS Entertainment", 0)
            )
            .verifyComplete();
    }

    //Project Reactor Essentials 24

}
