package org.nuxeo.ecm.platform.importer.queue.producer;

import java.util.List;

import org.nuxeo.ecm.core.api.blobholder.BlobHolder;
import org.nuxeo.ecm.platform.importer.source.SourceNode;

public class SourceNodeProducer extends AbstractProducer implements Producer {

    protected final SourceNode root;

    public SourceNodeProducer(SourceNode root) {
        this.root = root;
    }

    @Override
    public void run() {
        try {
            submit(root);
            completed = true;
        } catch (Exception e) {
            log.error("Error during sourceNode processing", e);
            error = e;
        }
    }

    protected void submit(SourceNode node) throws Exception {
        BlobHolder bh = node.getBlobHolder();
        if (bh!=null) {
            dispatch(bh);
        }
        List<SourceNode> children = node.getChildren();
        if (children!=null) {
            for (SourceNode child : children) {
                submit(child);
            }
        }
    }

}
