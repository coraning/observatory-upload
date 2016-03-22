package ch.zhaw.mami.db;

import java.util.Date;

import org.bson.Document;

import ch.zhaw.mami.RuntimeConfiguration;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

public class UploadDB {
	private final RuntimeConfiguration runtimeConfiguration;
	private final static Object mutex = new Object();
	private final MongoCollection<Document> collection;
	private final MongoCollection<Document> lockCollection;

	public UploadDB(final RuntimeConfiguration runtimeConfiguration) {
		this.runtimeConfiguration = runtimeConfiguration;
		collection = runtimeConfiguration.getMongoClient()
				.getDatabase(runtimeConfiguration.getUploadDBName())
				.getCollection("uploads");
		lockCollection = runtimeConfiguration.getMongoClient()
				.getDatabase(runtimeConfiguration.getUploadDBName())
				.getCollection("locks");

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
