package org.playground;


import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class PlaygroundTests {

    @Test
    public void test() throws InterruptedException {

        EmitterProcessor<Integer> processor = EmitterProcessor.create();

        Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).delayElements(Duration.ofMillis(50)).subscribe(processor::onNext);

        Map<String, Integer> integers = new HashMap<>();

        Flux<Integer> f2 = Flux.<Integer>create(synchronousSink -> processor.buffer(Duration.ofMillis(200))
            .map(Flux::fromIterable).flatMap(buf -> buf.groupBy(num -> num % 2 == 0 ? "even" : "odd")
                .flatMap(group -> group.reduce(Integer::sum).doOnNext(integer -> {
                    Integer i = integers.merge(group.key(), integer, Integer::sum);
                    synchronousSink.next(i);
                }))).subscribe()).share().startWith(integers.values());

        Disposable sub = f2.subscribe((e) -> System.out.println("First " + e));
        Thread.sleep(5000);
        sub.dispose();
        Thread.sleep(1000);
        System.out.println("after sleep");
        f2.subscribe((e) -> System.out.println("Second " + e));

        Thread.sleep(1000);
    }

    @Test
    public void test1() throws InterruptedException {

        EmitterProcessor<Integer> processor = EmitterProcessor.create();

        Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).delayElements(Duration.ofMillis(50)).subscribe(processor::onNext);

        Map<String, Integer> integers = new HashMap<>();

        Flux<Integer> f2 = Flux.<Integer>create(synchronousSink -> processor.window(Duration.ofMillis(200))
            .flatMap(win -> win.groupBy(num -> num % 2 == 0 ? "even" : "odd")
                .flatMap(group -> group.reduce(Integer::sum).doOnNext(integer -> {
                    Integer i = integers.merge(group.key(), integer, Integer::sum);
                    synchronousSink.next(i);
                }))).subscribe()).share().startWith(integers.values());

        Disposable sub = f2.subscribe((e) -> System.out.println("First " + e));
        sub.dispose();
        Thread.sleep(5000);
        Thread.sleep(1000);
        System.out.println("after sleep");
        f2.subscribe((e) -> System.out.println("Second " + e));

        Thread.sleep(1000);
    }

}
