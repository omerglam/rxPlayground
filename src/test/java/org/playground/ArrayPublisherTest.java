package org.playground;

import org.junit.jupiter.api.Assertions;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

public class ArrayPublisherTest extends PublisherVerification<Long> {

    public ArrayPublisherTest() {
        super(new TestEnvironment());
    }

    //@Test
    public void checkAcceptanceCriteria() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);

        ArrayList<Long> list = new ArrayList<>(4);

        ArrayPublisher<Long> pub = new ArrayPublisher<>(new Long[]{1L,2L,3L,4L});

        pub.subscribe(new Subscriber<Long>() {
            int request = 2;

            @Override public void onSubscribe(Subscription s) {
                s.request(request);
                System.out.println("SUBSCRIBED " + s);
            }

            @Override public void onNext(Long integer) {
                System.out.println(integer);
                list.add(integer);

            }

            @Override public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override public void onComplete() {
                System.out.println("COMPLETED");
                latch.countDown();
            }
        });

        latch.await();
        Assertions.assertEquals(4,list.size());
    }

    @Override public Publisher<Long> createPublisher(long elements) {

        return new ArrayPublisher<Long>(LongStream.range(0,elements).boxed().toArray(Long[]::new));
    }

    @Override public Publisher<Long> createFailedPublisher() {
        return null;
    }
}
