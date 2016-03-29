package ch.zhaw.mami.db;

import org.bson.Document;

import ch.zhaw.mami.RuntimeConfiguration;

import com.mongodb.client.MongoCollection;

public class AuthDB {

    private final RuntimeConfiguration runtimeConfiguration;
    private final static Object mutex = new Object();
    private final MongoCollection<Document> collection;

    public AuthDB(final RuntimeConfiguration runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
        collection = runtimeConfiguration.getMongoClient()
                .getDatabase(runtimeConfiguration.getAuthDBName())
                .getCollection("api_keys");

    }

    public int getAccessLevel(final String apiKey) {
        if (apiKey == null) {
            return AccessLevels.ACCESS_NONE;
        }

        Document doc = new Document().append("api_key", apiKey);
        for (Document result : collection.find(doc)) {
            Object accessLevel = result.get("access_level");
            if (accessLevel != null) {
                if (accessLevel instanceof Double) {
                    return new Integer((int) Math.floor((Double) accessLevel));
                }
            }
        }
        return AccessLevels.ACCESS_NONE;
    }

    public String getName(final String apiKey) {
        if (apiKey == null) {
            return "n/a";
        }

        Document doc = new Document().append("api_key", apiKey);
        for (Document result : collection.find(doc)) {
            Object name = result.get("name");
            if (name != null) {
                if (name instanceof String) {
                    return (String) name;
                }
            }
        }
        return "n/a";
    }

    public void revoke(final String apiKey) {
        Document doc = new Document().append("api_key", apiKey);
        collection.deleteMany(doc);
    }
}
