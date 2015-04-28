package org.nuxeo.ecm.platform.importer.queue.manager;

import java.util.concurrent.BlockingQueue;

import org.nuxeo.ecm.core.api.blobholder.BlobHolder;

public interface QueuesManager {

    BlockingQueue<BlobHolder> getQueue(int idx);

    int dispatch(BlobHolder bh);

    int getNBConsumers();

}