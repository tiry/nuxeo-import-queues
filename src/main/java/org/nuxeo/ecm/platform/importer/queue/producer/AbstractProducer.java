package org.nuxeo.ecm.platform.importer.queue.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.blobholder.BlobHolder;
import org.nuxeo.ecm.platform.importer.queue.AbstractTaskRunner;
import org.nuxeo.ecm.platform.importer.queue.manager.QueuesManager;

public abstract class AbstractProducer extends AbstractTaskRunner implements Producer {

    protected final static Log log = LogFactory.getLog(AbstractProducer.class);

    protected QueuesManager qm;

    @Override
    public void init(QueuesManager qm) {
        this.qm = qm;
    }

    protected void dispatch(BlobHolder bh) {
        qm.dispatch(bh);
        incrementProcessed();
    }

}