package org.nuxeo.ecm.platform.importer.queue.manager;

import java.util.Random;

import org.nuxeo.ecm.core.api.blobholder.BlobHolder;

public class RandomQueuesManager extends AbstractQueuesManager {

    protected final Random rand;

    public RandomQueuesManager(int size) {
        super(size);
        rand = new Random(System.currentTimeMillis());
    }

    @Override
    protected int getTargetQueue(BlobHolder bh, int nbQueues) {
        return rand.nextInt(nbQueues);
    }
}
