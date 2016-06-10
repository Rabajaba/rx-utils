package com.rabajaba.rxutils;

import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PortionObservableTest {
    private static final int LIMIT = 10;
    private static final int TOTAL = 50;
    private static final RuntimeException testExceptionInstance = new RuntimeException();
    private static List<PortionObservable.Range> ranges;

    @Test
    public void basicPortionTest() throws Exception {
        ranges = new CopyOnWriteArrayList<>();
        TestSubscriber<String> subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(LIMIT,
                        range -> {
                            ranges.add(range);
                            return Observable.range(range.getOffset(), range.getLimit())
                                    .filter(i -> i < TOTAL)
                                    .map(i -> "" + i);
                        })
                .subscribe(subs);
        subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        List<String> data = subs.getOnNextEvents();
        assertEquals("Size of emitted items should be same as 'getNextPortion' function emitted", TOTAL, data.size());
        for (int i = 0; i < TOTAL; i++) {
            assertEquals("Data should be responded in a sequential order", "" + i, data.get(i));
        }
        // now we need to validate what set of ranges were asked
        assertEquals("This test reads the whole stream, so should have all available ranges", (TOTAL / LIMIT) + 1, ranges.size());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(0), ranges.get(0).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(10), ranges.get(1).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(20), ranges.get(2).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(30), ranges.get(3).getOffset());
        assertEquals("If last portion is of the same size as limit, than another portion will be triggered", Integer.valueOf(40), ranges.get(4).getOffset());
        assertEquals("Last call is always expecting for an empty portion", Integer.valueOf(50), ranges.get(5).getOffset());
    }

    @Test
    public void noAdditionalPortionTest() throws Exception {
        ranges = new CopyOnWriteArrayList<>();
        TestSubscriber<Integer> subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(LIMIT,
                        range -> {
                            ranges.add(range);
                            return Observable.range(range.getOffset(), range.getLimit())
                                    .filter(i -> i < 45);
                        })
                .subscribe(subs);
        subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        List<Integer> data = subs.getOnNextEvents();
        assertEquals("Size of emitted items should be same as 'getNextPortion' function emitted", 45, data.size());
        for (int i = 0; i < 45; i++) {
            assertEquals("Data should be responded in a sequential order", Integer.valueOf(i), data.get(i));
        }
        // now we need to validate what set of ranges were asked
        assertEquals("This test reads the whole stream, so should have all available ranges", 5, ranges.size());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(0), ranges.get(0).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(10), ranges.get(1).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(20), ranges.get(2).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(30), ranges.get(3).getOffset());
        assertEquals("If last portion is less than a limit, than no additional portion should be read", Integer.valueOf(40), ranges.get(4).getOffset());
    }

    @Test
    public void noAdditionalPortionTestFastpath() throws Exception {
        ranges = new CopyOnWriteArrayList<>();
        TestSubscriber<Integer> subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(LIMIT,
                        range -> {
                            ranges.add(range);
                            return Observable.range(range.getOffset(), range.getLimit())
                                    .filter(i -> i < 45);
                        })
                .count()
                .subscribe(subs);
        subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        List<Integer> data = subs.getOnNextEvents();
        assertEquals("Size of emitted items should be same as 'getNextPortion' function emitted", 1, data.size());
        assertEquals("Data should be responded in a sequential order", Integer.valueOf(45), data.get(0));
        // now we need to validate what set of ranges were asked
        assertEquals("This test reads the whole stream, so should have all available ranges", 5, ranges.size());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(0), ranges.get(0).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(10), ranges.get(1).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(20), ranges.get(2).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(30), ranges.get(3).getOffset());
        assertEquals("If last portion is less than a limit, than no additional portion should be read", Integer.valueOf(40), ranges.get(4).getOffset());
    }

    @Test
    public void finishOnlyOnEmptyTest() throws Exception {
        ranges = new CopyOnWriteArrayList<>();
        TestSubscriber<Integer> subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(LIMIT,
                        range -> {
                            ranges.add(range);
                            return Observable.range(range.getOffset(), range.getLimit())
                                    .filter(i -> i < 45);
                        },
                        PortionObservable.FinishMode.ONLY_ON_EMPTY,
                        Schedulers.immediate())
                .subscribe(subs);
        subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        List<Integer> data = subs.getOnNextEvents();
        assertEquals("Size of emitted items should be same as 'getNextPortion' function emitted", 45, data.size());
        for (int i = 0; i < 45; i++) {
            assertEquals("Data should be responded in a sequential order", Integer.valueOf(i), data.get(i));
        }
        // now we need to validate what set of ranges were asked
        assertEquals("This test reads the whole stream, so should have all available ranges", 6, ranges.size());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(0), ranges.get(0).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(10), ranges.get(1).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(20), ranges.get(2).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(30), ranges.get(3).getOffset());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(40), ranges.get(4).getOffset());
        assertEquals("For 'finishOnlyOnEmpty' there should be always last portion being empty", Integer.valueOf(50), ranges.get(5).getOffset());
    }

    @Test
    public void finishOnlyOnEmptyTest2() throws Exception {
        // just turn off additional pages generation
        ranges = new CopyOnWriteArrayList<>();
        TestSubscriber<Integer> subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(10,
                        range -> {
                            ranges.add(range);
                            return Observable.range(range.getOffset(), range.getLimit())
                                    .filter(i -> i % 2 == 0)
                                    .filter(i -> i < 1000);
                        },
                        PortionObservable.FinishMode.ONLY_ON_EMPTY,
                        Schedulers.io())
                .buffer(7)
                .concatMap(list -> Observable.from(list))
                .subscribe(subs);
        subs.awaitTerminalEvent(1000, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        List<Integer> data = subs.getOnNextEvents();
        assertEquals("Size of emitted items should be same as 'getNextPortion' function emitted", 500, data.size());
        for (int i = 0; i < 500; i++) {
            assertEquals("Data should be responded in a sequential order", Integer.valueOf(i * 2), data.get(i));
        }
        // now we need to validate what set of ranges were asked
        assertEquals("This test reads the whole stream, so should have all available ranges", 101, ranges.size());
    }

    @Test
    public void finishOnlyOnEmptyTest3() throws Exception {
        ranges = new CopyOnWriteArrayList<>();
        TestSubscriber<Integer> subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(10,
                        range -> {
                            ranges.add(range);
                            return Observable.range(range.getOffset(), range.getLimit())
                                    .filter(i -> i % 10 == 0)
                                    .filter(i -> i < 1000);
                        },
                        PortionObservable.FinishMode.ONLY_ON_EMPTY,
                        Schedulers.io())
                .buffer(7)
                .concatMap(list -> Observable.from(list))
                .subscribe(subs);
        subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        List<Integer> data = subs.getOnNextEvents();
        assertEquals("Size of emitted items should be same as 'getNextPortion' function emitted", 100, data.size());
        for (int i = 0; i < 100; i++) {
            assertEquals("Data should be responded in a sequential order", Integer.valueOf(i * 10), data.get(i));
        }
    }

    @Test
    public void observableShouldStopReadingPortionsWhenUnsubscribed() throws Exception {
        final int lastElementSubscribed = 15;
        ranges = new CopyOnWriteArrayList<>();
        TestSubscriber<String> subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(LIMIT,
                        range -> {
                            ranges.add(range);
                            return Observable.range(range.getOffset(), range.getLimit())
                                    .map(i -> "" + i);
                        })
                .takeUntil(i -> i.equals("" + lastElementSubscribed)) // we unsubscribed here, so no more portions should be read
                .subscribe(subs);
        subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        subs.assertValueCount(lastElementSubscribed + 1);
        List<String> data = subs.getOnNextEvents();
        for (int i = 0; i <= lastElementSubscribed; i++) {
            assertEquals("Data should be responded in a sequential order", "" + i, data.get(i));
        }
        // now we need to validate what set of ranges were asked
        assertEquals("Only portions prior to fail and failed portion should be called", 2, ranges.size());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(0), ranges.get(0).getOffset());
        assertEquals("Failed portion should be called", Integer.valueOf(10), ranges.get(1).getOffset());
    }

    @Test
    public void noRecursionShouldAppear() throws Exception {
        final int moreThanStackSizeValue = 1000000;
        CountDownLatch latch = new CountDownLatch(1);
        // this code should never fail
        PortionObservable
                .newInstance(1,
                        range -> Observable.range(range.getOffset(), range.getLimit())
                                .filter(i -> i < moreThanStackSizeValue))
                .subscribe(n -> {
                        },
                        RuntimeException::new,
                        latch::countDown);
        assertTrue("Counting from 0 to 1000000 should succeed fast", latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void noRecursionNoBackpressureShouldAppear() throws Exception {
        final int moreThanStackSizeValue = 100000;
        TestSubscriber<Integer> subs = new TestSubscriber<>();
        // this code should never fail
        PortionObservable
                .newInstance(1,
                        range -> Observable.range(range.getOffset(), range.getLimit())
                                .filter(i -> i < moreThanStackSizeValue),
                        PortionObservable.FinishMode.DEFAULT,
                        Schedulers.io())
                .buffer(2)
                .concatMap(i -> Observable.from(i)
                        .observeOn(Schedulers.io())
                )
                .subscribe(subs);
        subs.awaitTerminalEvent(10, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        subs.assertValueCount(moreThanStackSizeValue);
    }

    /**
     * This test emulates very poor portion behavior in {@link com.rabajaba.rxutils.PortionObservable.FinishMode#ONLY_ON_EMPTY}.
     * ConcatMap supports backpressure of only 2 items, so with buffer(10) on top of it initial request will be 20, while pages are responding with following amounts:
     * 3 + 1 + 1 + 1 + 9 + 9 + 9 + 9 + 9 + 9 <br/>
     * So when two pages in a row will respond 9 items and will not support proper Backpressure too much of data would be pushed to ConcatMap leading to backpressure.
     * This test is dedicated to cover this issue.
     *
     * @throws Exception
     */
    @Test
    public void backpressureOnGoodBufferShouldBeHandled() throws Exception {
        // launching test a lot of times to validate probability of multithreading issues
        for (int j = 0; j < 100; j++) {
            TestSubscriber<Integer> subs = new TestSubscriber<>();
            // this code should never fail
            PortionObservable
                    .newInstance(10,
                            range -> Observable.range(range.getOffset(), range.getLimit())
                                    .filter(i -> range.getOffset().equals(0) && i < 3 ||
                                            range.getOffset().equals(10) && i < 11 ||
                                            range.getOffset().equals(20) && i < 21 ||
                                            range.getOffset().equals(30) && i < 31 ||
                                            range.getOffset().equals(40) && i < 49 ||
                                            range.getOffset().equals(50) && i < 59 ||
                                            range.getOffset().equals(60) && i < 69 ||
                                            range.getOffset().equals(70) && i < 79 ||
                                            range.getOffset().equals(80) && i < 89 ||
                                            range.getOffset().equals(90) && i < 99)
                                    .filter(i -> i < 100),
                            PortionObservable.FinishMode.ONLY_ON_EMPTY,
                            Schedulers.io()
                    )
                    .buffer(10)
                    .concatMap(i -> Observable.from(i)
                            .observeOn(Schedulers.io())
                    )
                    .subscribe(subs);
            subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
            subs.assertNoErrors();
            subs.assertCompleted();
            subs.assertValueCount(3 + 1 + 1 + 1 + 9 + 9 + 9 + 9 + 9 + 9);
        }
    }

    /**
     * Validates use case when backpressure requests less data, than one page responds
     *
     * @throws Exception
     */
    @Test
    public void backpressureOnBadBufferShouldBeHandled() throws Exception {
        // launching test a lot of times to validate probability of multithreading issues
        for (int j = 0; j < 100; j++) {
            TestSubscriber<Integer> subs = new TestSubscriber<>();
            // this code should never fail
            PortionObservable
                    .newInstance(10,
                            range -> Observable.range(range.getOffset(), range.getLimit())
                                    .filter(i -> range.getOffset().equals(0) && i < 3 ||
                                            range.getOffset().equals(10) && i < 11 ||
                                            range.getOffset().equals(20) && i < 21 ||
                                            range.getOffset().equals(30) && i < 31 ||
                                            range.getOffset().equals(40) && i < 49 ||
                                            range.getOffset().equals(50) && i < 59 ||
                                            range.getOffset().equals(60) && i < 69 ||
                                            range.getOffset().equals(70) && i < 79 ||
                                            range.getOffset().equals(80) && i < 89 ||
                                            range.getOffset().equals(90) && i < 99)
                                    .filter(i -> i < 100),
                            PortionObservable.FinishMode.ONLY_ON_EMPTY,
                            Schedulers.io()
                    )
                    .buffer(3)
                    .concatMap(i -> Observable.from(i)
                            .observeOn(Schedulers.io())
                    )
                    .subscribe(subs);
            subs.awaitTerminalEvent(100, TimeUnit.SECONDS);
            subs.assertNoErrors();
            subs.assertCompleted();
            subs.assertValueCount(3 + 1 + 1 + 1 + 9 + 9 + 9 + 9 + 9 + 9);
        }
    }

    /**
     * Validates use case when backpressure requests less data, than one page responds
     *
     * @throws Exception
     */
    @Test
    public void backpressureOnBadBufferShouldBeHandled2() throws Exception {
        // launching test a lot of times to validate probability of multithreading issues
        for (int j = 0; j < 100; j++) {
            TestSubscriber<Integer> subs = new TestSubscriber<>();
            // this code should never fail
            PortionObservable
                    .newInstance(100,
                            range -> Observable.range(range.getOffset(), range.getLimit())
                                    .filter(i -> i < 100),
                            PortionObservable.FinishMode.ONLY_ON_EMPTY,
                            Schedulers.io()
                    )
                    .buffer(3)
                    .concatMap(i -> Observable.from(i)
                            .observeOn(Schedulers.io())
                    )
                    .subscribe(subs);
            subs.awaitTerminalEvent(100, TimeUnit.SECONDS);
            subs.assertNoErrors();
            subs.assertCompleted();
            subs.assertValueCount(100);
        }
    }

    @Test
    public void noRecursionShouldAppear3() throws Exception {
        final int moreThanStackSizeValue = 100000;
        TestSubscriber<Integer> subs = new TestSubscriber<>();
        // this code should never fail
        PortionObservable
                .newInstance(1,
                        range -> Observable.range(range.getOffset(), range.getLimit())
                                .filter(i -> i < moreThanStackSizeValue),
                        PortionObservable.FinishMode.DEFAULT,
                        null)
                .buffer(2)
                .concatMap(i -> Observable.from(i)
                        .observeOn(Schedulers.io())
                )
                .subscribe(subs);
        subs.awaitTerminalEvent(10, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        subs.assertValueCount(moreThanStackSizeValue);
    }

    @Test
    public void errorReadingPortionShouldNotCallNextPortion() throws Exception {
        ranges = new CopyOnWriteArrayList<>();
        TestSubscriber<String> subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(LIMIT,
                        range -> {
                            ranges.add(range);
                            return Observable.range(range.getOffset(), range.getLimit())
                                    .map(i -> {
                                        if (i.equals(15)) {
                                            throw testExceptionInstance;
                                        }
                                        return i;
                                    })
                                    .filter(i -> i < TOTAL)
                                    .map(i -> "" + i);
                        })
                .subscribe(subs);
        subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
        subs.assertError(testExceptionInstance);
        // now we need to validate what set of ranges were asked
        assertEquals("Only portions prior to fail and failed portion should be called", 2, ranges.size());
        assertEquals("Ranges should be in sequential order", Integer.valueOf(0), ranges.get(0).getOffset());
        assertEquals("Failed portion should be called", Integer.valueOf(10), ranges.get(1).getOffset());
    }

    @Test
    public void validateSchedulers() throws Exception {
        TestSubscriber<Integer> subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(LIMIT,
                        range -> Observable.range(range.getOffset(), range.getLimit(), Schedulers.computation())
                                .filter(i -> i < TOTAL))
                .subscribe(subs);
        subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        List<Integer> data = subs.getOnNextEvents();
        assertEquals("Size of emitted items should be same as 'getNextPortion' function emitted", TOTAL, data.size());
        assertTrue("Subscriber should see same scheduler, as 'getNextPortion' scheduler, if it is set", subs.getLastSeenThread().getName().contains("RxComputation"));

        subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(LIMIT,
                        range -> Observable.range(range.getOffset(), range.getLimit())
                                .filter(i -> i < TOTAL))
                .subscribe(subs);
        subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        data = subs.getOnNextEvents();
        assertEquals("Size of emitted items should be same as 'getNextPortion' function emitted", TOTAL, data.size());
        // here you can check java.util.concurrent.Executors.defaultThreadFactory() behavior, to validate naming convention
        assertTrue("If no scheduler is set, than system one should be used", subs.getLastSeenThread().getName().contains("pool-"));

        subs = new TestSubscriber<>();
        PortionObservable
                .newInstance(LIMIT, range -> Observable.range(range.getOffset(), range.getLimit())
                                .filter(i -> i < TOTAL),
                        PortionObservable.FinishMode.DEFAULT,
                        Schedulers.io())
                .subscribe(subs);
        subs.awaitTerminalEvent(1, TimeUnit.SECONDS);
        subs.assertNoErrors();
        subs.assertCompleted();
        data = subs.getOnNextEvents();
        assertEquals("Size of emitted items should be same as 'getNextPortion' function emitted", TOTAL, data.size());
        assertTrue("If scheduler of observable is set and 'getPortion' have no scheduler, than initial scheduler should be used", subs.getLastSeenThread().getName().contains("RxIo"));
    }

    @Test
    @Ignore // performance test
    public void portionsPerformanceTest() throws Exception {
        for (int j = 0; j < 100; j++) {
            long start = System.currentTimeMillis();
            final int moreThanStackSizeValue = 1000000;
            PortionObservable
                    .newInstance(100,
                            range -> Observable.range(range.getOffset(), range.getLimit())
                                    .filter(i -> i < moreThanStackSizeValue))
                    .toBlocking()
                    .subscribe();
            System.out.println(System.currentTimeMillis() - start);
        }
    }

    @Test
    @Ignore // performance test
    public void usualRxJavaPerformanceTest() throws Exception {
        for (int j = 0; j < 100; j++) {
            final int moreThanStackSizeValue = 1000000;
            long start = System.currentTimeMillis();
            Observable.range(0, 10000)
                    .concatMap(i -> Observable.range(i, 100))
                    .filter(i -> i < moreThanStackSizeValue)
                    .toBlocking()
                    .subscribe();
            System.out.println(System.currentTimeMillis() - start);
        }
    }
}