package org.nuxeo.ecm.platform.importer.queue;

public abstract class AbstractTaskRunner implements TaskRunner {

    protected boolean completed = false;

    protected Exception error;

    protected long nbProcessed = 0;

    protected void incrementProcessed() {
        nbProcessed++;
    }

    @Override
    public boolean isCompleted() {
        return completed;
    }

    @Override
    public boolean isTerminated() {
        return completed || getError()!=null;
    }

    @Override
    public Exception getError() {
        return error;
    }

    @Override
    public long getNbProcessed() {
        return nbProcessed;
    }

}
