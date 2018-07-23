package edu.umich.gaialib;

// A simplest Future interface

public class TerraFuture<T> {

    boolean isDone = false;

    public TerraFuture() {
    }

    public boolean isDone() {
        return isDone;
    }

    public void setDone(boolean done) {
        isDone = done;
    }
}
