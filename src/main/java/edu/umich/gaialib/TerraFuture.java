package edu.umich.gaialib;

import com.google.common.util.concurrent.ListenableFuture;
import edu.umich.gaialib.gaiaprotos.HelloReply;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TerraFuture<T> implements ListenableFuture<T> {
    private ListenableFuture<T> helloReply;

    public TerraFuture(ListenableFuture<T> helloReply) {
        this.helloReply = helloReply;
    }

    public void addListener(Runnable runnable, Executor executor) {
        helloReply.addListener(runnable, executor);
    }

    public boolean isDone() {
        return helloReply.isDone();
    }

    public boolean isCancelled() {
        return helloReply.isCancelled();
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return helloReply.cancel(mayInterruptIfRunning);
    }

    public T get() throws InterruptedException, ExecutionException {
        return helloReply.get();
    }

    public T get(long timeout, TimeUnit unit) throws
            InterruptedException, ExecutionException, TimeoutException {
        return helloReply.get(timeout, unit);
    }
}