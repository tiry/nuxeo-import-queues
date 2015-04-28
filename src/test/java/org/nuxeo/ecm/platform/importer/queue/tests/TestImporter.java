package org.nuxeo.ecm.platform.importer.queue.tests;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.importer.queue.QueueImporter;
import org.nuxeo.ecm.platform.importer.queue.manager.RandomQueuesManager;
import org.nuxeo.ecm.platform.importer.queue.producer.Producer;
import org.nuxeo.ecm.platform.importer.queue.producer.SourceNodeProducer;
import org.nuxeo.ecm.platform.importer.source.RandomTextSourceNode;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import com.google.inject.Inject;

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
public class TestImporter {

    @Inject
    CoreSession session;

    @Test
    public void shouldImport() {

        QueueImporter importer = new QueueImporter();

        RandomQueuesManager qm = new RandomQueuesManager(2);

        RandomTextSourceNode root = RandomTextSourceNode.init(1000, 1, true);

        Producer producer = new SourceNodeProducer(root);

        importer.importDocuments(producer, qm, session.getRootDocument(), 5);


    }

}
