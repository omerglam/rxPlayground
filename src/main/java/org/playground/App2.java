package org.playground;

import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.HashMap;
import java.util.function.Supplier;

public class App2 {
    private static Flux<HashMap<Tuple2<Side, Long>, State>> flux;

    public static void main(String... args) throws InterruptedException {

        EmitterProcessor<UpstreamEvent> processor = EmitterProcessor.create();

        Flux.just(
            new UpstreamEvent(Side.A, 1L, 10L),
            new UpstreamEvent(Side.B, 1L, 100L),
            new UpstreamEvent(Side.A, 2L, 1L),
            new UpstreamEvent(Side.A, 1L, -1L),
            new UpstreamEvent(Side.A, 3L, 1L),
            new UpstreamEvent(Side.A, 2L, 1L),
            new UpstreamEvent(Side.A, 5L, 90L),
            new UpstreamEvent(Side.A, 1L, -9L)
        ).delayElements(Duration.ofMillis(50)).subscribe(processor::onNext);

        // Aggregation flux
        flux = processor.window(Duration.ofMillis(200))
            .flatMap(window ->
                window.groupBy(UpstreamEvent::side)
                    .flatMap(sideGroup -> sideGroup.groupBy(UpstreamEvent::price)
                        .map(priceGroup ->
                            priceGroup.reduce(ImmutableSum.empty(), (sum, e) -> sum.add(e.changeInQuantity))
                                .map(sum -> Tuples.of(sideGroup.key(), priceGroup.key(), sum.sum())))
                        .collectList()
                        .flatMap(listOfMonos -> Mono.zip(listOfMonos, objs -> objs))
                    )).scanWith(HashMap::new, (hMap, objects) -> {

                hMap.entrySet().removeIf(entry -> entry.getValue().quantity <= 0); //Removing empty entries
                hMap.forEach((key, value) -> value.setModified(false));   //reset modified flag

                for (Object obj : objects) {
                    Tuple3<Side, Long, Long> tuple = (Tuple3<Side, Long, Long>) obj;

                    hMap.merge(Tuples.of(tuple.getT1(), tuple.getT2()), new State(tuple.getT3()),
                        (state, state2) -> {
                            State newState = new State(state.quantity + state2.quantity);
                            newState.setModified(true);
                            return newState;
                        });
                }
                return hMap;
            });

        //snapshot creation
        ReplayProcessor<HashMap<String, DownstreamEvent>> snapshot = flux.map(hMap -> {
            HashMap<String, DownstreamEvent> snapshotMap = new HashMap<>();

            hMap.forEach((key, value) -> {
                if (value.quantity > 0) {
                    snapshotMap.put(key.getT1().name() + "_" + key.getT2(),
                        new DownstreamEvent(key.getT1(), key.getT2(), value.quantity, Action.Insert));
                }
            });

            return snapshotMap;
        }).subscribeWith(ReplayProcessor.cacheLast());

        // Flux to send only modified entries
        Flux<DownstreamEvent> fluxa =
            flux.scanWith((Supplier<HashMap<String, DownstreamEvent>>) HashMap::new, (stateMap, newMap) -> {

                stateMap.entrySet().removeIf(entry -> entry.getValue().sum
                    <= 0); //Remove empty entries (the delete operation was sent in last iteration)

                newMap.forEach((key, value) -> {
                    String keyToMap = key.getT1().name() + "_" + key.getT2();
                    DownstreamEvent e = stateMap.get(keyToMap);

                    boolean isInsert = e == null && value.isInsertedNew();
                    boolean isUpdate = value.isModified() && value.quantity > 0;
                    boolean isDelete = value.isModified() && value.quantity <= 0;
                    boolean noChange = !isInsert && !isDelete && !isUpdate;

                    Action action = isInsert ? Action.Insert : isUpdate ? Action.Update : Action.Delete;

                    if (noChange) {
                        stateMap.remove(keyToMap); // Don't emit non changed entries
                    } else {
                        stateMap.put(keyToMap, new DownstreamEvent(key.getT1(), key.getT2(), value.quantity, action));
                    }
                });

                return stateMap;
            }).flatMapIterable(HashMap::values).share();


        Disposable sub = fluxa.startWith(snapshot.take(1).flatMapIterable(HashMap::values))
            .subscribe((e) -> System.out.println("First " + e));

        Thread.sleep(500);

        System.out.println("snapshot:");
        fluxa.startWith(snapshot.take(1).flatMapIterable(HashMap::values))
            .subscribe((e) -> System.out.println("Second " + e));

        Thread.sleep(2000);

        //TODO: order not guaranteed ? might return negative values:
        //        First [side=A, price=1, sum=-1, action=Insert]
        //        First [side=A, price=2, sum=2, action=Insert]
        //        First [side=A, price=3, sum=1, action=Insert]
        //        First [side=A, price=1, sum=-9, action=Insert]
        //        First [side=A, price=5, sum=90, action=Insert]
        //        snapshot:
        //        Second [side=A, price=2, sum=2, action=Insert]
        //        Second [side=B, price=1, sum=100, action=Insert]
        //        Second [side=A, price=3, sum=1, action=Insert]
        //        Second [side=A, price=5, sum=90, action=Insert]

    }


    enum Side {
        A, B
    }


    static class UpstreamEvent {

        private Side side;
        private Long price;

        private Long changeInQuantity;

        public UpstreamEvent(Side side, Long price, Long changeInQuantity) {
            this.side = side;
            this.price = price;
            this.changeInQuantity = changeInQuantity;
        }

        public Side side() {
            return side;
        }

        public Long price() {
            return price;
        }

        public Long changeInQuantity() {
            return changeInQuantity;
        }

        @Override public String toString() {
            return "UpstreamEvent{" +
                "side=" + side +
                ", price=" + price +
                ", changeInQuantity=" + changeInQuantity +
                '}';
        }
    }


    enum Action {
        Insert, Update, Delete
    }


    static class State {
        private Long quantity;
        private boolean modified;
        private boolean insertedNew;

        public State(Long quantity) {
            this.quantity = quantity;
            this.modified = false;
            this.insertedNew = true;
        }

        public Long quantity() {
            return quantity;
        }

        public boolean isModified() {
            return modified;
        }

        public boolean isInsertedNew() {
            return insertedNew;
        }

        public void setModified(boolean modified) {
            this.insertedNew = false;
            this.modified = modified;
        }

        @Override public String toString() {
            return "State{" +
                "quantity=" + quantity +
                ", modified=" + modified +
                '}';
        }
    }


    static class DownstreamEvent {
        private Side side;
        private Long price;
        private Long sum;
        private Action action;

        public DownstreamEvent(Side side, Long price, Long sum, Action action) {
            this.side = side;
            this.price = price;
            this.sum = sum;
            this.action = action;
        }

        public Side side() {
            return side;
        }

        public Long sum() {
            return sum;
        }

        public Action action() {
            return action;
        }

        public Long price() {
            return price;
        }

        @Override public String toString() {
            return "[" +
                "side=" + side +
                ", price=" + price +
                ", sum=" + sum +
                ", action=" + action +
                ']';
        }
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
