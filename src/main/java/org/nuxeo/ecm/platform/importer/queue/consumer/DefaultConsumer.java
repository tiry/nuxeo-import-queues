package org.nuxeo.ecm.platform.importer.queue.consumer;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.blobholder.BlobHolder;
import org.nuxeo.ecm.core.api.model.PropertyNotFoundException;
import org.nuxeo.ecm.platform.filemanager.api.FileManager;
import org.nuxeo.runtime.api.Framework;

public class DefaultConsumer extends AbstractConsumer {

    protected FileManager fileManager;

    protected final String rootPath;

    protected static final Log log = LogFactory.getLog(DefaultConsumer.class);

    public DefaultConsumer(DocumentModel root, int batchSize, BlockingQueue<BlobHolder> queue) {
        super(root, batchSize, queue);
        fileManager = Framework.getService(FileManager.class);
        rootPath = root.getPathAsString();
    }

    protected String getType() {
        return "File";
    }

    @Override
    protected void process(CoreSession session, BlobHolder bh) {

        String fileName = null;
        String name = null;
        Blob blob = bh.getBlob();
        if (blob != null) {
            fileName = blob.getFilename();
        }
        Map<String, Serializable> props = bh.getProperties();
        if(props!=null) {
            name = (String) props.get("name");
        }
        if (name == null) {
            name = fileName;
        } else if (fileName == null) {
            fileName = name;
        }

        DocumentModel doc = session.createDocumentModel(rootPath, name, getType());

        doc.setProperty("dublincore", "title", name);
        doc.setProperty("file", "filename", fileName);
        doc.setProperty("file", "content", bh.getBlob());

        if (bh != null) {
            doc = setDocumentProperties(session, bh.getProperties(), doc);
        }

        doc = session.createDocument(doc);
    }

    protected DocumentModel setDocumentProperties(CoreSession session, Map<String, Serializable> properties,
            DocumentModel doc) throws ClientException {
        if (properties != null) {

            for (Map.Entry<String, Serializable> entry : properties.entrySet()) {
                try {
                    doc.setPropertyValue(entry.getKey(), entry.getValue());
                } catch (PropertyNotFoundException e) {
                    String message = String.format("Property '%s' not found on document type: %s. Skipping it.",
                            entry.getKey(), doc.getType());
                    log.debug(message);
                }
            }
            doc = session.saveDocument(doc);
        }
        return doc;
    }
}
