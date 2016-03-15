package ch.zhaw.mami.db;

import org.bson.Document;

import ch.zhaw.mami.RuntimeConfiguration;

import com.mongodb.client.MongoCollection;

public class UploadDB {
	private final RuntimeConfiguration runtimeConfiguration;
	private final static Object mutex = new Object();
	private final MongoCollection<Document> collection;

	public UploadDB(final RuntimeConfiguration runtimeConfiguration) {
		this.runtimeConfiguration = runtimeConfiguration;
		collection = runtimeConfiguration.getMongoClient()
				.getDatabase(runtimeConfiguration.getUploadDBName())
				.getCollection("uploads");

	}

	public void completeUpload(final String path, final String sha1) {
		Document queryDoc = new Document();
		Document updateDoc = new Document();
		Document modDoc = new Document();

		queryDoc.append("path", path);

		modDoc.append("complete", true);
		modDoc.append("sha1", sha1);

		updateDoc.append("$set", modDoc);

		collection.updateOne(queryDoc, updateDoc);
	}

	public void insertUpload(final String path, final String jsonData,
			final String name) {
		Document metaDoc = Document.parse(jsonData);
		Document doc = new Document();
		doc.append("path", path);
		doc.append("meta", metaDoc);
		doc.append("sha1", "");
		doc.append("complete", false);
		doc.append("uploader", name);

		collection.insertOne(doc);
	}
}
