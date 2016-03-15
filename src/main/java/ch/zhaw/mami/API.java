package ch.zhaw.mami;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.security.MessageDigest;
import java.util.Date;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import ch.zhaw.mami.db.AccessLevels;
import ch.zhaw.mami.db.AuthDB;
import ch.zhaw.mami.db.LogDB;
import ch.zhaw.mami.db.UploadDB;

import com.sun.jersey.multipart.FormDataParam;

@Path("/hdfs")
public class API {

	public RuntimeConfiguration runtimeConfiguration;
	private final static Logger logger = LogManager.getLogger(API.class);

	public static byte value;
	public static Object mutex = new Object();
	public static boolean next = false;

	private final AuthDB authDB;
	private final UploadDB uploadDB;
	private final LogDB logDB;

	public API() throws IOException {
		API.logger.entry();
		runtimeConfiguration = RuntimeConfiguration.getInstance();
		System.out.println(runtimeConfiguration.getPathPrefix());
		authDB = runtimeConfiguration.getAuthDB();
		uploadDB = runtimeConfiguration.getUploadDB();
		logDB = runtimeConfiguration.getLogDB();
		API.logger.exit();
	}

	private Response accessError() {
		API.logger.entry();
		return API.logger.exit(Response.status(401)
				.entity("Invalid API token or insufficient access!")
				.type(MediaType.TEXT_PLAIN).build());
	}

	@Path("mgmt/accessLevel")
	@GET
	public Response accessLevel(@HeaderParam("X-API-KEY") final String apiKey) {
		API.logger.entry(apiKey);
		int accessLevel = authDB.getAccessLevel(apiKey);
		return API.logger.exit(Response.ok("" + accessLevel,
				MediaType.TEXT_PLAIN).build());
	}

	public ResponseBuilder allowCORS(final ResponseBuilder rb) {
		return API.logger.exit(rb.header("Access-Control-Allow-Origin",
				"*"));
	}

	@Path("fs/bin/{path:.+}")
	@GET
	public Response bin(@HeaderParam("X-API-KEY") final String apiKey,
			@PathParam("path") final String path) {
		API.logger.entry(apiKey, path);

		if (!checkAccessLevel(apiKey, AccessLevels.ACCESS_FS_READ)) {
			return API.logger
					.exit(API.logger.exit(accessError()));
		}

		logDB.insertLogEntry(path, "bin", authDB.getName(apiKey));

		if (!Util.validatePath(path)) {
			return API.logger
					.exit(clientError("Invalid path (contains illegal characters or too long)"));
		}

		try {

			org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path(
					runtimeConfiguration.getPathPrefix() + path);

			if (!runtimeConfiguration.getFileSystem().exists(pt)) {
				return generic404("File not found: " + pt.getName());
			}

			if (!runtimeConfiguration.getFileSystem().isFile(pt)) {
				return API.logger.exit(generic404("Not a file: "
						+ pt.getName()));
			}

			final BufferedReader br = new BufferedReader(new InputStreamReader(
					runtimeConfiguration.getFileSystem().open(pt)));

			StreamingOutput so = new StreamingOutput() {

				@Override
				public void write(final OutputStream os) throws IOException,
						WebApplicationException {
					BufferedWriter bw = new BufferedWriter(
							new OutputStreamWriter(os));

					char[] chunk = new char[runtimeConfiguration.getChunkSize()];
					int read;
					API.logger.trace("Reading chunks...");
					while ((read = br.read(chunk)) > 0) {
						API.logger.trace("got chunk of size: " + read);
						bw.write(chunk);
						API.logger.trace("wrote chunk");
					}
					bw.flush();

				}

			};

			return API.logger.exit(Response.ok(so)
					.type(MediaType.APPLICATION_OCTET_STREAM).build());
		} catch (Exception ex) {
			API.logger.catching(ex);
			return API.logger.exit(internalError());
		}
	}

	public boolean checkAccessLevel(final String apiKey, final int level) {

		int accessLevel = authDB.getAccessLevel(apiKey);
		if ((accessLevel & level) == level) {
			return true;
		} else {
			return false;
		}
	}

	private Response clientError(final String message) {
		API.logger.entry(message);
		return API.logger.exit(Response.status(401).entity(message)
				.type(MediaType.TEXT_PLAIN).build());
	}

	public Response generic404(final String message) {
		API.logger.entry(message);
		return API.logger.exit(Response.status(404).entity(message)
				.type(MediaType.TEXT_PLAIN).build());
	}

	public Response generic500(final String message) {
		API.logger.entry(message);
		return API.logger.exit(Response.status(500).entity(message)
				.type(MediaType.TEXT_PLAIN).build());
	}

	private Response generic500(final String string, final Exception ex) {
		API.logger.entry(string, ex);
		ex.printStackTrace();
		return API.logger.exit(generic500(string));
	}

	@SuppressWarnings("deprecation")
	private Response internalError() {
		API.logger.entry();
		API.logger.error("** INTERNAL ERROR **");
		return API.logger
				.exit(generic500("Internal Error - Please contact administrator. Timestamp: "
						+ new Date().toGMTString()));
	}

	@Path("fs/ls/{path:.+}")
	@GET
	public Response ls(@HeaderParam("X-API-KEY") final String apiKey,
			@PathParam("path") final String path) {

		API.logger.entry(apiKey, path);

		if (!checkAccessLevel(apiKey, AccessLevels.ACCESS_FS_READ)) {
			return API.logger.exit(accessError());
		}

		logDB.insertLogEntry(path, "ls", authDB.getName(apiKey));

		if (!Util.validatePath(path)) {
			return API.logger
					.exit(clientError("Invalid path (contains illegal characters or too long)"));
		}

		try {
			org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path(
					runtimeConfiguration.getPathPrefix() + path);

			FileSystem fs = runtimeConfiguration.getFileSystem();

			if (!fs.exists(pt)) {
				return API.logger
						.exit(generic404("Directory not found: " + pt.getName()));
			}

			if (!fs.isDirectory(pt)) {
				return API.logger.exit(generic404("Not a directory: "
						+ pt.getName()));
			}

			RemoteIterator<LocatedFileStatus> files = fs.listFiles(pt, false);
			JSONArray arr = new JSONArray();
			while (files.hasNext()) {
				LocatedFileStatus lfs = files.next();
				arr.put(lfs.getPath().getName());
			}
			return API.logger.exit(allowCORS(
					Response.ok(arr.toString(), MediaType.APPLICATION_JSON))
					.build());
		} catch (Exception ex) {
			API.logger.catching(ex);
			return API.logger.exit(generic500("Internal Error"));
		}
	}

	@Path("fs/lsR/{path:.+}")
	@GET
	public Response lsR(@HeaderParam("X-API-KEY") final String apiKey,
			@PathParam("path") final String path) {

		API.logger.entry(apiKey, path);

		if (!checkAccessLevel(apiKey, AccessLevels.ACCESS_FS_READ)) {
			return API.logger.exit(accessError());
		}

		logDB.insertLogEntry(path, "lsR", authDB.getName(apiKey));

		if (!Util.validatePath(path)) {
			return API.logger
					.exit(clientError("Invalid path (contains illegal characters or too long)"));
		}

		try {
			org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path(
					runtimeConfiguration.getPathPrefix() + path);

			FileSystem fs = runtimeConfiguration.getFileSystem();

			if (!fs.exists(pt)) {
				return API.logger
						.exit(generic404("Directory not found: " + pt.getName()));
			}

			if (!fs.isDirectory(pt)) {
				return API.logger.exit(generic404("Not a directory: "
						+ pt.getName()));
			}

			RemoteIterator<LocatedFileStatus> files = fs.listFiles(pt, true);
			JSONArray arr = new JSONArray();
			while (files.hasNext()) {
				LocatedFileStatus lfs = files.next();
				arr.put(lfs.getPath().toString());
			}
			return API.logger.exit(Response.ok(arr.toString(),
					MediaType.TEXT_PLAIN).build());
		} catch (Exception ex) {
			API.logger.catching(ex);
			return API.logger.exit(internalError());
		}
	}

	@Path("fs/put/{path:.+}")
	@POST
	@Consumes({ MediaType.APPLICATION_OCTET_STREAM })
	public Response put(@HeaderParam("X-API-KEY") final String apiKey,
			@PathParam("path") final String path, final InputStream data) {

		API.logger.entry(apiKey, path, data);

		if (!checkAccessLevel(apiKey, AccessLevels.ACCESS_FS_ADMIN)) {
			return API.logger.exit(accessError());
		}

		logDB.insertLogEntry(path, "put", authDB.getName(apiKey));

		if (!Util.validatePath(path)) {
			return API.logger
					.exit(clientError("Invalid path (contains illegal characters or too long)"));
		}

		try {
			org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path(
					runtimeConfiguration.getPathPrefix() + path);

			FileSystem fs = runtimeConfiguration.getFileSystem();
			OutputStream os = fs.create(pt);

			byte chunk[] = new byte[runtimeConfiguration.getChunkSize()];
			int read;
			while ((read = data.read(chunk)) > 0) {
				os.write(chunk, 0, read);
			}
			os.flush();
			os.close();
			data.close();

			return API.logger.exit(Response.ok(pt.toString(),
					MediaType.TEXT_PLAIN).build());
		} catch (Exception ex) {
			API.logger.catching(ex);
			return API.logger.exit(internalError());
		}
	}

	@GET
	@javax.ws.rs.Path("fs/raw/{path:.+}")
	public Response raw(@HeaderParam("X-API-KEY") final String apiKey,
			@PathParam("path") final String path) {

		API.logger.entry(apiKey, path);

		if (!checkAccessLevel(apiKey, AccessLevels.ACCESS_FS_READ)) {
			return API.logger.exit(accessError());
		}

		logDB.insertLogEntry(path, "raw", authDB.getName(apiKey));

		if (!Util.validatePath(path)) {
			return API.logger
					.exit(clientError("Invalid path (contains illegal characters or too long)"));
		}

		org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path(
				runtimeConfiguration.getPathPrefix() + path);

		try {

			if (!runtimeConfiguration.getFileSystem().exists(pt)) {
				return API.logger.exit(generic404("File not found: "
						+ pt.getName()));
			}

			if (!runtimeConfiguration.getFileSystem().isFile(pt)) {
				return API.logger.exit(generic404("Not a file: "
						+ pt.getName()));
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					runtimeConfiguration.getFileSystem().open(pt)));

			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line + "\n");
			}
			br.close();

			return API.logger.exit(allowCORS(
					Response.ok(sb.toString(), MediaType.TEXT_PLAIN)).build());

		} catch (Exception ex) {
			API.logger.catching(ex);
			return API.logger.exit(internalError());
		}
	}

	@Path("mgmt/revoke")
	@POST
	public Response revoke(@HeaderParam("X-API-KEY") final String apiKey) {
		API.logger.entry(apiKey);
		authDB.revoke(apiKey);
		return API.logger.exit(Response.ok("OK", MediaType.TEXT_PLAIN)
				.build());
	}

	@Path("fs/rm/{path:.+}")
	@DELETE
	public Response rm(@HeaderParam("X-API_KEY") final String apiKey,
			@PathParam("path") final String path) {
		API.logger.entry(apiKey, path);

		if (!checkAccessLevel(apiKey, AccessLevels.ACCESS_FS_ADMIN)) {
			return API.logger.exit(accessError());
		}

		logDB.insertLogEntry(path, "rm", authDB.getName(apiKey));

		if (!Util.validatePath(path)) {
			return API.logger
					.exit(clientError("Invalid path (contains illegal characters or too long)"));
		}

		try {
			org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path(
					runtimeConfiguration.getPathPrefix() + path);

			FileSystem fs = runtimeConfiguration.getFileSystem();

			if (!fs.exists(pt)) {
				return API.logger.exit(generic404("File not found: "
						+ pt.getName()));
			}

			if (!fs.isFile(pt)) {
				return API.logger.exit(generic404("Not a file: "
						+ pt.getName()));
			}

			if (!fs.delete(pt, false)) {
				return API.logger.exit(clientError("Delete failed!"));
			}

			return API.logger.exit(Response.ok("OK",
					MediaType.TEXT_PLAIN).build());

		} catch (Exception ex) {
			API.logger.catching(ex);
			return API.logger.exit(internalError());
		}
	}

	@Path("fs/rmR/{path:.+}")
	@DELETE
	public Response rmR(@HeaderParam("X-API_KEY") final String apiKey,
			@PathParam("path") final String path) {
		API.logger.entry(apiKey, path);

		if (!checkAccessLevel(apiKey, AccessLevels.ACCESS_FS_ADMIN)) {
			return API.logger.exit(accessError());
		}

		logDB.insertLogEntry(path, "rmR", authDB.getName(apiKey));

		if (!Util.validatePath(path)) {
			return API.logger
					.exit(clientError("Invalid path (contains illegal characters or too long)"));
		}

		try {
			org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path(
					runtimeConfiguration.getPathPrefix() + path);

			FileSystem fs = runtimeConfiguration.getFileSystem();

			if (!fs.exists(pt)) {
				return API.logger.exit(generic404("File not found: "
						+ pt.getName()));
			}

			if (!fs.isFile(pt)) {
				return API.logger.exit(generic404("Not a file: "
						+ pt.getName()));
			}

			if (!fs.delete(pt, true)) {
				return API.logger.exit(clientError("Delete failed!"));
			}

			return API.logger.exit(Response.ok("OK",
					MediaType.TEXT_PLAIN).build());

		} catch (Exception ex) {
			API.logger.catching(ex);
			return API.logger.exit(internalError());
		}
	}

	@Path("status")
	@GET
	public Response status() {
		API.logger.entry();
		return API.logger.exit(Response.ok("RUNNING",
				MediaType.TEXT_PLAIN).build());
	}

	@Path("fs/touch/{path:.+}")
	@GET
	public Response touch(@HeaderParam("X-API-KEY") final String apiKey,
			@PathParam("path") final String path) {

		API.logger.entry(apiKey, path);

		if (!checkAccessLevel(apiKey, AccessLevels.ACCESS_FS_ADMIN)) {
			return API.logger.exit(accessError());
		}

		logDB.insertLogEntry(path, "touch", authDB.getName(apiKey));

		if (!Util.validatePath(path)) {
			return API.logger
					.exit(clientError("Invalid path (contains illegal characters or too long)"));
		}

		org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path(
				runtimeConfiguration.getPathPrefix() + path);

		try {
			FileSystem fs = runtimeConfiguration.getFileSystem();

			if (fs.exists(pt)) {
				return API.logger
						.exit(clientError("Path already exists."));
			}

			fs.create(pt);
			return API.logger.exit(Response.ok(pt.toString(),
					MediaType.TEXT_PLAIN).build());
		} catch (Exception ex) {
			API.logger.catching(ex);
			return API.logger.exit(internalError());
		}
	}

	@Path("up/{fileName}")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response upload(@FormDataParam("meta") final String meta,
			@FormDataParam("data") InputStream data,
			@PathParam("fileName") final String fileName,
			@HeaderParam("X-API-KEY") final String apiKey) {
		API.logger.entry(meta, data);

		if (!checkAccessLevel(apiKey, AccessLevels.ACCESS_FS_WRITE)) {
			return API.logger.exit(accessError());
		}

		logDB.insertLogEntry(fileName, "up", authDB.getName(apiKey));

		OutputStream os = null;

		try {

			JSONObject obj = null;

			try {
				obj = new JSONObject(meta);
			} catch (Exception ex) {
				return API.logger.exit(clientError("Invalid JSON"));
			}

			if (obj.getString("msmntCampaign") == null
					|| obj.getString("format") == null) {
				return API.logger
						.exit(clientError("Invalid JSON. Need `msmntCampaign` and `format`."));
			}

			if (!Util.validatePathPart(obj.getString("msmntCampaign"))
					|| !Util.validatePathPart(obj.getString("format"))) {
				return API.logger
						.exit(clientError("Invalid `msmntCampaign` or invalid `format` (contain illegal characters or too long)"));
			}

			if (!Util.validateFileName(fileName)) {
				return API.logger
						.exit(clientError("Invalid file name (contains illegal characters or too long)"));
			}

			org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path(
					runtimeConfiguration.getPathPrefix()
							+ obj.getString("msmntCampaign") + "/"
							+ obj.getString("format") + "/" + fileName);

			FileSystem fs = runtimeConfiguration.getFileSystem();

			if (fs.exists(pt)) {
				return API.logger
						.exit(clientError("Path already exists."));
			}

			MessageDigest md = MessageDigest.getInstance("SHA-1");

			uploadDB.insertUpload(pt.toString(), meta, authDB.getName(apiKey));

			os = fs.create(pt);

			byte chunk[] = new byte[runtimeConfiguration.getChunkSize()];
			int read;
			while ((read = data.read(chunk)) > 0) {
				md.update(chunk, 0, read);
				os.write(chunk, 0, read);
			}

			os.flush();
			os.close();
			data.close();

			os = null;
			data = null;

			String digest = Util.byteArr2HexStr(md.digest());

			uploadDB.completeUpload(pt.toString(), digest);

			return API.logger.exit(Response.ok("OK").build());

		} catch (Exception ex) {
			API.logger.catching(ex);
			return API.logger.exit(internalError());
		}
	}
}