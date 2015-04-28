package org.nuxeo.ecm.platform.importer.queue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.platform.importer.queue.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.queue.consumer.DefaultConsumer;
import org.nuxeo.ecm.platform.importer.queue.manager.QueuesManager;
import org.nuxeo.ecm.platform.importer.queue.producer.Producer;

public class QueueImporter {

    protected static final Log log = LogFactory.getLog(QueueImporter.class);

    public void importDocuments(Producer producer, QueuesManager manager, DocumentModel root, int batchSize) {

        producer.init(manager);

        // start the producer
        Thread p = new Thread(producer);
        p.start();

        List<Thread> consumerThreads = new ArrayList<Thread>();
        List<Consumer> consumers = new ArrayList<Consumer>();

        for (int i = 0; i < manager.getNBConsumers(); i++) {
            Consumer c = new DefaultConsumer(root, batchSize, manager.getQueue(i));
            consumers.add(c);
            Thread ct = new Thread(c);
            ct.start();
            consumerThreads.add(ct);
        }

        long i=0;
        try {
            while (!producer.isTerminated()) {
                Thread.sleep(50);
                i++;
                System.out.println("waiting for producer to be completed " + producer.getNbProcessed());
            }
        } catch (InterruptedException e) {
            log.error("Error while waiting for producder", e);
        }

        Exception pe = producer.getError();
        if (pe!=null) {
            log.error("Error during producer execution", pe);
            for (Consumer c : consumers) {
                c.mustStop();
            }
        } else {
            for (Consumer c : consumers) {
                c.canStop();
            }
        }

        boolean terminated = false;

        try {
            while (!terminated) {
                Thread.sleep(100);
                terminated = true;
                int idx = 0;
                for (Consumer c : consumers) {
                    idx++;
                    System.out.println("waiting for consumer "+ idx + " to be completed :" + c.getNbProcessed() + " -- " + c.getImmediateThroughput() + " docs/s -- " + c.getThroughput() + "docs/s");
                    terminated = terminated && c.isTerminated();
                }
            }
        } catch (InterruptedException e) {
            log.error("Error while waiting for consumers", e);
        }

        System.out.println("End of import process");

    }

}
