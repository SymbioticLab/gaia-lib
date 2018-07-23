package edu.umich.gaialib;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TerraFutureComplex<T> implements ListenableFuture<T> {
    private ListenableFuture<T> someFuture;

    public TerraFutureComplex(ListenableFuture<T> someFuture) {
        this.someFuture = someFuture;
    }

    public void addListener(Runnable runnable, Executor executor) {
        someFuture.addListener(runnable, executor);
    }

    public boolean isDone() {
        return someFuture.isDone();
    }

    public boolean isCancelled() {
        return someFuture.isCancelled();
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return someFuture.cancel(mayInterruptIfRunning);
    }

    public T get() throws InterruptedException, ExecutionException {
        return someFuture.get();
    }

    public T get(long timeout, TimeUnit unit) throws
            InterruptedException, ExecutionException, TimeoutException {
        return someFuture.get(timeout, unit);
    }
}