package ch.zhaw.mami.db;

import java.util.Date;

import org.bson.Document;

import ch.zhaw.mami.RuntimeConfiguration;

import com.mongodb.client.MongoCollection;

public class LogDB {

    private final MongoCollection<Document> collection;

    public LogDB(final RuntimeConfiguration runtimeConfiguration) {
        collection = runtimeConfiguration.getMongoClient()
                .getDatabase(runtimeConfiguration.getLogDBName())
                .getCollection("log");

    }

    public void insertLogEntry(final String path, final String action,
            final String name) {

        Document doc = new Document();
        doc.append("path", path);
        doc.append("action", action);
        doc.append("name", name);
        doc.append("timestamp", new Date().getTime() / 1000);
        collection.insertOne(doc);
    }
}
