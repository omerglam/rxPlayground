package org.playground;

import reactor.core.Disposable;
import reactor.core.publisher.*;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class App {
    private static Flux flux;

    public static void main(String... args) throws InterruptedException {

        EmitterProcessor<Integer> processor = EmitterProcessor.create();

        //Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        Flux.just(1,2,3,5,7,9,11,13,15,17,19,21)
            .delayElements(Duration.ofMillis(50))
            .subscribe(processor::onNext);


        flux =
            processor.window(Duration.ofMillis(200))
            .flatMap(window ->
                window.groupBy(num -> num % 2 == 0 ? "even" : "odd")
                    .map(group -> group.reduce(ImmutableSum.empty(),
                        (sum, e) -> {
                            // here we have happens-before guarantees
                            // and serialized access so we may careless
                            // about concurrent access

                            return sum.add(e);
                        })
                        .map(sum -> Tuples.of(group.key(),
                            sum.sum())))
                    .collectList()
                    .flatMap(listOfMonos -> Mono.zip(listOfMonos, objs -> objs))
            )
            .scanWith((Supplier<HashMap<String, Long>>) HashMap::new, (hm, objs) -> {
                //					kind of rolling cache;
                for (Object o : objs) {
                    Tuple2<String, Long> tuple = (Tuple2<String, Long>) o;
                    hm.merge(tuple.getT1(), tuple.getT2(), Long::sum);
                }
                return hm;
            }).distinct(
                HashMap::keySet,
                HashSet::new,
                (store, key) -> {
                    if (store.equals(key)) {
                        return false;
                    }

                    store.addAll(key);
                    return true;
                },
                HashSet::clear).subscribeWith(ReplayProcessor.cacheLast());
                //.flatMap(i -> Flux.fromIterable(i.values()))
                //.distinct(aLong -> aLong, HashSet::new).subscribeWith(ReplayProcessor.cacheLast());
//                .map(stringLongHashMap -> Flux.just(stringLongHashMap.values()).startWith((s) -> {
//                s.onNext(stringLongHashMap.values());
//                s.onComplete();
//            }))

            //.subscribeWith(ReplayProcessor.cacheLast());
            //.flatMap(Flux::just)
                /*.startWith(stringLongHashMap))*///
            //.flatMap(Function.identity()).subscribeWith(ReplayProcessor.cacheLast());
            //.map(stringLongHashMap -> stringLongHashMap.values()).subscribeWith(ReplayProcessor.cacheLast());

        Disposable sub = flux.subscribe((e) -> System.out.println("First " + e));
        Thread.sleep(2000);
        sub.dispose();
        Thread.sleep(1000);
        System.out.println("after sleep");
        flux.subscribe((e) -> System.out.println("Second " + e));

        Thread.sleep(1000);
    }

    static class ImmutableSum {

        private final static ImmutableSum EMPTY = new ImmutableSum(0, 0);

        private final long count;
        private final long sum;

        ImmutableSum(long count, long sum) {
            this.count = count;
            this.sum = sum;
        }

        ImmutableSum add(long element) {
            return new ImmutableSum(count + 1, sum + element);
        }

        long sum() {
            return sum;
        }

        static ImmutableSum empty() {
            return EMPTY;
        }
    }
}
