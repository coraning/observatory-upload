package ch.zhaw.mami.db;

import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import ch.zhaw.mami.RuntimeConfiguration;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

public class UploadDB {

    private final RuntimeConfiguration runtimeConfiguration;
    private final static Object mutex = new Object();
    private final MongoCollection<Document> collection;
    private final MongoCollection<Document> lockCollection;
    private final MongoCollection<Document> errorCollection;

    private final static Logger logger = LogManager.getLogger(UploadDB.class);

    public UploadDB(final RuntimeConfiguration runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
        collection = runtimeConfiguration.getMongoClient()
                .getDatabase(runtimeConfiguration.getUploadDBName())
                .getCollection("uploads");
        lockCollection = runtimeConfiguration.getMongoClient()
                .getDatabase(runtimeConfiguration.getUploadDBName())
                .getCollection("locks");
        errorCollection = runtimeConfiguration.getMongoClient()
                .getDatabase(runtimeConfiguration.getUploadDBName())
                .getCollection("upload_errors");

    }

    public void completeSeqUpload(final String path, final String seqKey,
            final String sha1) {
        synchronized (UploadDB.mutex) {
            Document queryDoc = new Document();
            Document updateDoc = new Document();
            Document modDoc = new Document();

            queryDoc.append("path", path);
            queryDoc.append("seqKey", seqKey);

            modDoc.append("complete", true);
            modDoc.append("sha1", sha1);

            updateDoc.append("$set", modDoc);

            collection.updateOne(queryDoc, updateDoc);
        }
    }

    public void completeUpload(final String path, final String sha1) {
        synchronized (UploadDB.mutex) {
            Document queryDoc = new Document();
            Document updateDoc = new Document();
            Document modDoc = new Document();

            queryDoc.append("path", path);

            modDoc.append("complete", true);
            modDoc.append("sha1", sha1);

            updateDoc.append("$set", modDoc);

            collection.updateOne(queryDoc, updateDoc);
        }
    }

    public boolean getLock(final String path) {
        /*
         * TODO: this is not atomic if other processes write to the lock
         * collection. Is there some kind of insertIfNotExists (that is atomic)?
         */
        synchronized (UploadDB.mutex) {
            Document queryDoc = new Document();
            queryDoc.append("path", path);

            /* Entry with path exists. */
            FindIterable<Document> results = lockCollection.find(queryDoc);
            for (Document doc : results) {
                return false;
            }

            /* Does not exist. Let's create it then */

            Document doc = new Document();
            doc.append("path", path);
            doc.append("timestamp", new Date().getTime() / 1000);

            lockCollection.insertOne(doc);

            return true;
        }
    }

    public Document getUploadEntry(final String path) {
        synchronized (UploadDB.mutex) {
            Document queryDoc = new Document();

            queryDoc.append("path", path);

            FindIterable<Document> results = collection.find(queryDoc);
            for (Document doc : results) {
                return doc;
            }

            return null;
        }
    }

    public void insertError(final String path, String seqKey, final String msg) {
        synchronized (UploadDB.mutex) {
            try {
                if (seqKey == null) {
                    seqKey = "";
                }

                Document doc = new Document();
                doc.append("path", path);
                doc.append("seqKey", seqKey);
                doc.append("msg", msg);
                doc.append("timestamp", new Date().getTime() / 1000);

                errorCollection.insertOne(doc);
            } catch (Exception ex) {
                UploadDB.logger.catching(ex);
                UploadDB.logger.fatal("Could not insert error into DB!");
            }
        }
    }

    public boolean insertSeqUpload(final String path, final String jsonData,
            final String seqKey, final String name) {
        synchronized (UploadDB.mutex) {

            if (seqUploadExists(path, seqKey)) {
                return false;
            }

            Document metaDoc = Document.parse(jsonData);
            Document doc = new Document();
            doc.append("path", path);
            doc.append("meta", metaDoc);
            doc.append("sha1", "");
            doc.append("complete", false);
            doc.append("seqKey", seqKey);
            doc.append("uploader", name);
            doc.append("timestamp", new Date().getTime() / 1000);

            collection.insertOne(doc);

            return true;
        }
    }

    public boolean insertUpload(final String path, final String jsonData,
            final String name) {
        synchronized (UploadDB.mutex) {
            if (uploadExists(path)) {
                return false;
            }

            Document metaDoc = Document.parse(jsonData);
            Document doc = new Document();
            doc.append("path", path);
            doc.append("meta", metaDoc);
            doc.append("sha1", "");
            doc.append("complete", false);
            doc.append("uploader", name);
            doc.append("timestamp", new Date().getTime() / 1000);

            collection.insertOne(doc);

            return true;
        }
    }

    public boolean releaseLock(final String path) {
        synchronized (UploadDB.mutex) {
            Document queryDoc = new Document();
            queryDoc.append("path", path);

            lockCollection.deleteOne(queryDoc);
        }
        return false;
    }

    public boolean seqUploadExists(final String path, final String seqKey) {
        synchronized (UploadDB.mutex) {
            Document queryDoc = new Document();

            queryDoc.append("path", path);
            queryDoc.append("seqKey", seqKey);

            FindIterable<Document> results = collection.find(queryDoc);
            for (Document doc : results) {
                return true;
            }

            return false;
        }
    }

    public boolean uploadExists(final String path) {
        synchronized (UploadDB.mutex) {
            Document queryDoc = new Document();

            queryDoc.append("path", path);

            FindIterable<Document> results = collection.find(queryDoc);
            for (Document doc : results) {
                return true;
            }

            return false;
        }
    }
}
