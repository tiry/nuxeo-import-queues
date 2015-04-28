package org.nuxeo.ecm.platform.importer.queue.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.nuxeo.ecm.core.api.blobholder.BlobHolder;

public abstract class AbstractQueuesManager implements QueuesManager {

    List<BlockingQueue<BlobHolder>> queues = new ArrayList<BlockingQueue<BlobHolder>>();

    public AbstractQueuesManager(int size) {
        for (int i = 0; i < size ; i++){
            queues.add(new LinkedBlockingQueue<BlobHolder>());
        }
    }

    @Override
    public BlockingQueue<BlobHolder> getQueue(int idx) {
        return queues.get(idx);
    }

    @Override
    public int dispatch(BlobHolder bh) {
        int idx = getTargetQueue(bh, queues.size());
        getQueue(idx).offer(bh);
        return idx;
    }

    protected abstract int getTargetQueue(BlobHolder bh, int nbQueues);

    @Override
    public int getNBConsumers() {
        return queues.size();
    }
}
