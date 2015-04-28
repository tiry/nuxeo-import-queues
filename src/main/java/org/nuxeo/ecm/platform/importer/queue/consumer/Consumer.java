package org.nuxeo.ecm.platform.importer.queue.consumer;

import org.nuxeo.ecm.platform.importer.queue.TaskRunner;

public interface Consumer extends TaskRunner {

    public void mustStop();

    public void canStop();

    public double getImmediateThroughput();

    public double getThroughput();

}
