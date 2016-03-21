package ch.zhaw.mami;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import ch.zhaw.mami.db.AuthDB;
import ch.zhaw.mami.db.LogDB;
import ch.zhaw.mami.db.UploadDB;

import com.mongodb.MongoClient;

/**
 * Runtime configuration. Singleton class. Also serves as a factory. TODO: Maybe
 * out-source factory logic?
 * 
 * @author mroman
 * 
 */
public class RuntimeConfiguration {
	private static RuntimeConfiguration instance;
	private static String path = "hdfs://srv-lab-t-425:9000/test";
	private static String authDBName = "auth";
	private static String uploadDBName = "uploads";
	private static String logDBName = "log";
	private static String url = "http://localhost:9998/";

	private final static String cfgHDFS_PATH = "HDFS_PATH";
	private final static String cfgAUTH_DB_NAME = "AUTH_DB_NAME";
	private final static String cfgLOG_DB_NAME = "LOG_DB_NAME";
	private final static String cfgUPLOAD_DB_NAME = "UPLOAD_DB_NAME";
	private final static String cfgURL = "URL";
	private final static String propCfgPath = "MAMI_HDFS_CFG_PATH";
	private final AuthDB authDB;
	private final UploadDB uploadDB;
	private final LogDB logDB;

	private static MongoClient mongoClient;

	public static RuntimeConfiguration getInstance() throws IOException {

		if (RuntimeConfiguration.instance != null) {
			return RuntimeConfiguration.instance;
		}

		String cfgPath = "/etc/hdfs-mami/srvc.cfg";

		if (System.getProperty(RuntimeConfiguration.propCfgPath) != null) {
			cfgPath = System.getProperty(RuntimeConfiguration.propCfgPath);
		} else {
			System.out.println("Using default /etc/hdfs-mami/srvc.cfg!");
		}

		Properties props = new Properties();
		InputStream is = new FileInputStream(new File(cfgPath));
		props.load(is);
		is.close();

		if (props.getProperty(RuntimeConfiguration.cfgHDFS_PATH) != null) {
			RuntimeConfiguration.path = props
					.getProperty(RuntimeConfiguration.cfgHDFS_PATH);
		}

		if (props.getProperty(RuntimeConfiguration.cfgAUTH_DB_NAME) != null) {
			RuntimeConfiguration.authDBName = props
					.getProperty(RuntimeConfiguration.cfgAUTH_DB_NAME);
		}

		if (props.getProperty(RuntimeConfiguration.cfgUPLOAD_DB_NAME) != null) {
			RuntimeConfiguration.uploadDBName = props
					.getProperty(RuntimeConfiguration.cfgUPLOAD_DB_NAME);
		}

		if (props.getProperty(RuntimeConfiguration.cfgLOG_DB_NAME) != null) {
			RuntimeConfiguration.logDBName = props
					.getProperty(RuntimeConfiguration.cfgLOG_DB_NAME);
		}

		if (props.getProperty(RuntimeConfiguration.cfgURL) != null) {
			RuntimeConfiguration.url = props
					.getProperty(RuntimeConfiguration.cfgURL);
		}

		return RuntimeConfiguration.instance = new RuntimeConfiguration();
	}

	private final int chunkSize = 1024;

	private FileSystem fileSystem;

	private RuntimeConfiguration() throws IOException {
		authDB = new AuthDB(this);
		uploadDB = new UploadDB(this);
		logDB = new LogDB(this);
	}

	public AuthDB getAuthDB() {
		return authDB;
	}

	public String getAuthDBName() {
		return RuntimeConfiguration.authDBName;
	}

	public String getAuthDBUri() {
		return RuntimeConfiguration.authDBName;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public FileSystem getFileSystem() throws IOException {
		if (fileSystem == null) {
			fileSystem = FileSystem.get(getFSConfiguration());
		}
		return fileSystem;
	}

	public Configuration getFSConfiguration() {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", RuntimeConfiguration.path);
		conf.set("dfs.replication", "1");
		return conf;
	}

	public LogDB getLogDB() {
		return logDB;
	}

	public String getLogDBName() {
		return RuntimeConfiguration.logDBName;
	}

	public MongoClient getMongoClient() {
		if (RuntimeConfiguration.mongoClient == null) {
			RuntimeConfiguration.mongoClient = new MongoClient();
		}

		return RuntimeConfiguration.mongoClient;
	}

	public String getPathPrefix() {
		return RuntimeConfiguration.path + "/";
	}

	public UploadDB getUploadDB() {
		return uploadDB;
	}

	public String getUploadDBName() {
		return RuntimeConfiguration.uploadDBName;
	}

	public String getURL() {
		return RuntimeConfiguration.url;
	}

}
