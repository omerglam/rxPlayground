package org.playground;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ArrayPublisher<T> implements Publisher<T> {
    private final T[] source;

    public ArrayPublisher(T[] source) {
        this.source = source;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        Subscription sub = new Subscription() {
            AtomicInteger sentItems = new AtomicInteger(0);
            AtomicBoolean isDone = new AtomicBoolean(false);
            AtomicLong requested =new AtomicLong(0);

            @Override
            public void request(long n) {

                if(isDone.get()){
                    return;
                }

                if(n <= 0) {
                    isDone.set(true);
                    s.onError(new IllegalArgumentException("n must be greater than 0"));
                    return;
                }

                long newRequested = requested.get();

                if(requested.get() != Long.MAX_VALUE) {
                    newRequested += n;

                    if(newRequested < 0) { //Overflow
                        requested.set(Long.MAX_VALUE);
                    }
                }

                if( requested.get() > 0 ){
                    requested.set(newRequested);
                    return;
                }

                requested.set(newRequested);

                for (int sent =0;
                     sentItems.get() < source.length
                     && !isDone.get()
                     && sent < requested.get();
                     sentItems.incrementAndGet(), sent++) {
                    s.onNext(source[sentItems.get()]);
                }

                requested.set(requested.addAndGet(-requested.get()));

                if(sentItems.get() == source.length){
                    isDone.set(true);
                    s.onComplete();
                }

            }

            @Override public void cancel() {
                isDone.set(true);
            }
        };


        s.onSubscribe(sub);

    }

}
