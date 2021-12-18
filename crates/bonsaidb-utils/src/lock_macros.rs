/// Acquires an `async-lock::Mutex` by first attempting `try_lock()` and then
/// falling back on `lock().await`. This is proven to be faster than simply
/// calling `lock().await` [in our
/// benchmarks](https://github.com/khonsulabs/async-locking-benchmarks/).
#[macro_export]
macro_rules! fast_async_lock {
    ($mutex:expr) => {{
        if let Some(locked) = $mutex.try_lock() {
            locked
        } else {
            $mutex.lock().await
        }
    }};
}

/// Acquires a read handle to an `async-lock::RwLock` by first attempting
/// `try_read()` and then falling back on `read().await`. This is proven to be
/// faster than simply calling `read().await` [in our
/// benchmarks](https://github.com/khonsulabs/async-locking-benchmarks/).
#[macro_export]
macro_rules! fast_async_read {
    ($rwlock:expr) => {{
        if let Some(locked) = $rwlock.try_read() {
            locked
        } else {
            $rwlock.read().await
        }
    }};
}

/// Acquires a write handle to an `async-lock::RwLock` by first attempting
/// `try_write()` and then falling back on `write().await`. This is proven to be
/// faster than simply calling `write().await` [in our
/// benchmarks](https://github.com/khonsulabs/async-locking-benchmarks/).
#[macro_export]
macro_rules! fast_async_write {
    ($rwlock:expr) => {{
        if let Some(locked) = $rwlock.try_write() {
            locked
        } else {
            $rwlock.write().await
        }
    }};
}
