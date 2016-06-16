# rx-utils
Utility classes for RxJava
Currently contains only PortionObservable class.

# PortionObservable

**Use cases**

1. Read whole data set of unknown size from some storage
2. Read data set which growth while reading
3. Read data from some data set, when pages are not guaranteed to be of same amount as limit

**Features**

1. Reactive
2. Support Backpressure
3. Supports two modes, when to stop: only when page is empty, or default - when page is less than a limit

**Sample**

`PortionObservable.newObservable(100, range -> dao.getProducts(range.getLimit(), range.getOffset())`
`.doOnNext(item -> log.debug("Got product {}.", item.getId()));`