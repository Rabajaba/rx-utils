package com.rabajaba.rxutils;

import rx.Observable;
import rx.Producer;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Func1;
import rx.internal.operators.BackpressureUtils;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dedicated to provide lazy portion reader abstraction, for instance for database read, when final size of the data initially is unknown.
 * <br/>
 * By calling {@link #newInstance(int, Func1, FinishMode, Scheduler)}} you're creating new Observable, which will lazily call 'getNextPortion' function
 * to retrieve next portion of data to emit into observable just created.
 * <br/>
 * Usage sample: <br/>
 * <code>
 * PortionObservable.newObservable(100, range -> dao.getProducts(range.getLimit(), range.getOffset()) <br/>
 * .doOnNext(item -> log.debug("Got product {}.", item.getId()));
 * </code> <br/>
 * <br/>
 * <b>Portion you're reading should have {@link rx.Scheduler} correctly set, if it's a blocking operation!</b>
 *
 * @param <T> type of instances this observable is using
 */
public final class PortionObservable<T> {
    /**
     * This thread pool is used for subscription of portion read, to avoid recursion calls
     */
    private static final ExecutorService POOL = Executors.newSingleThreadExecutor(Executors.defaultThreadFactory());
    private static final Scheduler SCHEDULER = Schedulers.from(POOL);
    private int limit;
    private Func1<Range, Observable<T>> getNextPortion;
    private Scheduler scheduler;
    private FinishMode mode;

    private PortionObservable(int limit, Func1<Range, Observable<T>> getNextPortion, Scheduler scheduler, FinishMode mode) {
        this.limit = limit;
        this.getNextPortion = getNextPortion;
        this.scheduler = scheduler;
        this.mode = mode;
    }

    /**
     * Creates Observable which reads portions from a function provided to it in a lazy way.
     * <br/>
     * Operates in it's own single-threaded scheduler,
     * so it will be never executed at {@link Schedulers#immediate()) unless you specify it at {@link #newInstance(int, Func1, Scheduler)}
     *
     * @param limit          how much data to ask for at once
     * @param getNextPortion function to retrieve data, should have it's own {@link rx.Scheduler} set, if it has blocking operations
     * @param <O>            type of instances you're interested in
     * @return new cold Observable, which will start to emit data only after it's subscribed
     */
    public static <O> Observable<O> newInstance(int limit, Func1<Range, Observable<O>> getNextPortion) {
        return new PortionObservable<>(limit, getNextPortion, SCHEDULER, FinishMode.DEFAULT)
                .newObservable();
    }

    /**
     * Creates Observable which reads portions from a function provided to it in a lazy way.
     * <br/>
     * Operates in scheduler you specify.
     *
     * @param limit          how much data to ask for at once
     * @param getNextPortion function to retrieve data, should have it's own {@link rx.Scheduler} set, if it has blocking operations
     * @param <O>            type of instances you're interested in
     * @param scheduler      scheduler this new observable will be executed on\9
     * @return new cold Observable, which will start to emit data only after it's subscribed
     */
    public static <O> Observable<O> newInstance(int limit, Func1<Range, Observable<O>> getNextPortion, FinishMode mode, Scheduler scheduler) {
        return new PortionObservable<>(limit, getNextPortion, scheduler == null ? SCHEDULER : scheduler, mode)
                .newObservable();
    }

    /**
     * Creates Observable which reads portions from a function provided to it in a lazy way.
     * <br/>
     * Operates in scheduler you specify.
     *
     * @param limit          how much data to ask for at once
     * @param getNextPortion function to retrieve data, should have it's own {@link rx.Scheduler} set, if it has blocking operations
     * @param <O>            type of instances you're interested in
     * @param scheduler      scheduler this new observable will be executed on
     * @return new cold Observable, which will start to emit data only after it's subscribed
     */
    public static <O> Observable<O> newInstance(int limit, Func1<Range, Observable<O>> getNextPortion, Scheduler scheduler) {
        return new PortionObservable<>(limit, getNextPortion, scheduler == null ? SCHEDULER : scheduler, FinishMode.DEFAULT)
                .newObservable();
    }

    private Observable<T> newObservable() {
        return Observable
                .create(new Observable.OnSubscribe<T>() {
                    @Override
                    public void call(Subscriber<? super T> subscriber) {
                        subscriber.setProducer(new PortionProducer(subscriber, mode, getNextPortion, limit, scheduler));
                    }
                })
                .subscribeOn(scheduler);
    }

    public enum FinishMode {
        /**
         * Latest portion will be considered as 'empty' when will respond with 0 size.
         * Means it's ok to read portions with less than limit amount.
         */
        ONLY_ON_EMPTY,
        /**
         * Latest portion will be considered as 'empty' when siz of it will be less than limit
         */
        DEFAULT
    }

    /**
     * Used as a helper class for {@link PortionObservable} to define which range should be read right now
     */
    public final static class Range {
        private Integer limit;
        private Integer offset;

        private Range(Integer limit, Integer offset) {
            this.limit = limit;
            this.offset = offset;
        }

        public Integer getLimit() {
            return limit;
        }

        public Integer getOffset() {
            return offset;
        }

        @Override
        public String toString() {
            return "Range (" + offset + ":" + (offset + limit) + ")";
        }
    }

    /**
     * Operates through pair of {@link #slowpathEmitter} and {@link #itemsToPush} where following happens: <br/>
     * <li/> {@link #slowpathEmitter} takes {@link Range} objects as an input to retrieve next additionalPages of data via {@link #function}
     * <li/> when pageCounter of data is received it's being send to {@link #itemsToPush} queue to control BackPressure issue
     * <li/> items to a {@link #subscriber} are always drained from {@link #itemsToPush} queue
     * <p>
     * <br/>
     * Extends {@link AtomicLong} to keep amount of data requested by subscriber, see {@link #request(long)}.
     */
    private final class PortionProducer extends AtomicLong implements Producer {
        final FinishMode mode;
        private final Subscriber<? super T> subscriber;
        final private AtomicInteger pageCounter = new AtomicInteger(0);
        private final Func1<Range, Observable<T>> function;
        private final Integer limit;
        // used for fastpath
        private Scheduler scheduler;

        // used for slowpath
        private final SerializedSubject<Range, Range> slowpathEmitter;
        private final Queue<T> itemsToPush = new ConcurrentLinkedQueue<>();
        private final Queue<Range> additionalPages = new ConcurrentLinkedQueue<>();
        // Stores start of {@link #slowpathEmitter}, so if it's over no more data on additionalPages would be read.
        private final AtomicBoolean finished = new AtomicBoolean(false);

        private PortionProducer(Subscriber<? super T> childSubscriber, FinishMode mode, Func1<Range, Observable<T>> function, Integer limit,
                                Scheduler scheduler) {
            this.subscriber = childSubscriber;
            this.mode = mode;
            this.limit = limit;
            this.scheduler = scheduler;
            this.slowpathEmitter = PublishSubject.<Range>create().toSerialized();
            this.function = function;
            // we're subscribing on it right away, expecting for onNext() to be called
            subscribeSlowpath(this.subscriber);
        }

        private void subscribeSlowpath(Subscriber<? super T> childSubscriber) {
            slowpathEmitter
                    .onBackpressureBuffer(Long.MAX_VALUE)
                    .doOnNext(page -> {
                        if (additionalPages.peek() == page) {
                            additionalPages.poll();
                        }
                    })
                    // sequential read of portions
                    .concatMap(range -> function.call(range)
                            .toList()
                    )
                    .takeUntil(page -> {
                        if (mode.equals(FinishMode.ONLY_ON_EMPTY)) {
                            return page.size() == 0;
                        } else {
                            return page.size() < limit;
                        }
                    })
                    .subscribe(page -> {
                                page.forEach(item -> itemsToPush.add(item));
                                // now we can drain queue
                                while (get() > 0 && itemsToPush.size() > 0) {
                                    if (decrementAndGet() >= 0) {
                                        T next = itemsToPush.poll();
                                        if (next == null) {
                                            incrementAndGet(); // setting value back, because no values available
                                            break;
                                        }
                                        childSubscriber.onNext(next);
                                    }
                                }
                                // all the things not send to a child are expecting to be hosted in a queue until next portion request
                                if (get() >= 0 && itemsToPush.size() == 0) {
                                    // if new portion is not full - we still should get enough of data to push to a child subscriber
                                    // that's why another portion is requested, even it's recursive call
                                    if (mode.equals(FinishMode.ONLY_ON_EMPTY) && page.size() < limit) {
                                        // need to validate we're not over requesting next additionalPages
                                        if (additionalPages.size() == 0) { // only one additional pageCounter at a time is allowed
                                            // trigger one more pageCounter
                                            Range range = new Range(limit, limit * this.pageCounter.getAndIncrement());
                                            additionalPages.add(range); // store state - we have additional page to be read
                                            slowpathEmitter.onNext(range);
                                        }
                                    }
                                }
                            }
                            ,
                            throwable -> {
                                // in case of error - just pass it to child
                                childSubscriber.unsubscribe();
                                childSubscriber.onError(throwable);
                            },
                            () -> {
                                finished.set(true);
                                if (itemsToPush.size() == 0) { // there are no remaining values
                                    // when complete just pass terminal event to child
                                    childSubscriber.unsubscribe();
                                    childSubscriber.onCompleted();
                                }
                            }
                    );
        }

        @Override
        public void request(long requestedAmount) {
            if (get() == Long.MAX_VALUE) {
                // already started with fast-path
                return;
            }
            if (requestedAmount == Long.MAX_VALUE && compareAndSet(0L, Long.MAX_VALUE)) {
                // fast-path without backpressure
                fastpath();
            } else if (requestedAmount > 0L) {
                BackpressureUtils.getAndAddRequest(this, requestedAmount);
                // backpressure is requested
                slowpath(requestedAmount);
            }
        }

        /**
         * Emits as many values as requested or remaining from the range, whichever is smaller.
         */
        void slowpath(long requestedAmount) {
            Subscriber<? super T> childSubscriber = this.subscriber;
            if (childSubscriber.isUnsubscribed()) {
                return;
            }
            if (finished.get()) {
                // no need to read portions, not we can just emit what's left
                if (itemsToPush.size() > 0) {
                    while (get() > 0) {
                        if (decrementAndGet() < 0) {
                            // something bad happened, just returning value back
                            incrementAndGet();
                            break;
                        }
                        T next = itemsToPush.poll();
                        if (next == null) {
                            if (!childSubscriber.isUnsubscribed()) {
                                // there is nothing in a queue, so we can shut down
                                childSubscriber.unsubscribe();
                                childSubscriber.onCompleted();
                            }
                        } else {
                            childSubscriber.onNext(next);
                        }
                    }
                } else {
                    if (!childSubscriber.isUnsubscribed()) {
                        // there is nothing in a queue, so we can shut down
                        childSubscriber.unsubscribe();
                        childSubscriber.onCompleted();
                    }
                }
            } else {
                // preparing set of portions to read
                if (requestedAmount < limit) {
                    slowpathEmitter.onNext(new Range(limit, limit * pageCounter.getAndIncrement()));
                } else {
                    for (int i = 0; i < requestedAmount / limit; i++) {
                        slowpathEmitter.onNext(new Range(limit, limit * pageCounter.getAndIncrement()));
                    }
                }
            }
        }

        /**
         * Emits all remaining values without decrementing the requested amount.
         * In case of backpressure is not supported will generate all values from given function.
         */
        void fastpath() {
            if (this.subscriber.isUnsubscribed()) {
                return;
            }
            // emit everything
            Observable.range(0, Integer.MAX_VALUE, scheduler)
                    .takeWhile(i -> !this.subscriber.isUnsubscribed())
                    // sequential read of portions
                    .concatMap(range -> function.call(new Range(limit, limit * pageCounter.getAndIncrement()))
                            .toList()
                    )
                    .takeUntil(page -> {
                        if (mode.equals(FinishMode.ONLY_ON_EMPTY)) {
                            return page.size() == 0;
                        } else {
                            return page.size() < limit;
                        }
                    })
                    .subscribe(list -> list.forEach(item -> subscriber.onNext(item)),
                            throwable -> {
                                subscriber.onError(throwable);
                                subscriber.unsubscribe();
                            },
                            () -> {
                                subscriber.onCompleted();
                                subscriber.unsubscribe();
                            }
                    );
        }
    }
}
