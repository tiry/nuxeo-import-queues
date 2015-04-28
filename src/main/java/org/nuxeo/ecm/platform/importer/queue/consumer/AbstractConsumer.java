package org.nuxeo.ecm.platform.importer.queue.consumer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.UnrestrictedSessionRunner;
import org.nuxeo.ecm.core.api.blobholder.BlobHolder;
import org.nuxeo.ecm.platform.importer.queue.AbstractTaskRunner;
import org.nuxeo.runtime.transaction.TransactionHelper;

public abstract class AbstractConsumer extends AbstractTaskRunner implements Consumer {

    protected final int batchSize;

    protected final String repositoryName;

    protected final BlockingQueue<BlobHolder> queue;

    protected boolean mustStop;

    protected boolean canStop;

    protected final DocumentRef rootRef;

    protected boolean started = false;

    public AbstractConsumer(DocumentModel root, int batchSize, BlockingQueue<BlobHolder> queue) {
        repositoryName = root.getRepositoryName();
        this.batchSize = batchSize;
        this.queue = queue;
        rootRef = root.getRef();
    }

    @Override
    public void run() {

        started=true;

        UnrestrictedSessionRunner runner = new UnrestrictedSessionRunner(repositoryName) {
            @Override
            public void run() throws ClientException {
                while (!mustStop) {
                    try {
                        BlobHolder bh = queue.poll(1, TimeUnit.SECONDS);
                        if(bh!=null){
                            process(session, bh);
                            incrementProcessed();
                            commitIfNeeded(session);
                        } else {
                            if (canStop) {
                                commit(session);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        error = e;
                        e.printStackTrace();
                        throw new ClientException(e);
                    }
                }
            }
        };

        TransactionHelper.startTransaction();
        try {
            runner.runUnrestricted();
            completed=true;
        } catch (ClientException e) {
            TransactionHelper.setTransactionRollbackOnly();
            error=e;
            throw e;
        } finally {
            TransactionHelper.commitOrRollbackTransaction();
        }
    }

    protected abstract void process(CoreSession session, BlobHolder bh) throws Exception ;

    protected void commitIfNeeded(CoreSession session) {
        if (nbProcessed % batchSize == 0) {
            commit(session);
        }
    }

    protected void commit(CoreSession session) {
        session.save();
        TransactionHelper.commitOrRollbackTransaction();
        TransactionHelper.startTransaction();
    }

    @Override
    public void mustStop() {
        mustStop = true;
    }


    @Override
    public void canStop() {
        canStop = true;
    }


    @Override
    public boolean isTerminated() {
        if (!started) {
            return true;
        }
        return super.isTerminated();
    }

}
