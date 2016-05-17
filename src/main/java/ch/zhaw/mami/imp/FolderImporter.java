package ch.zhaw.mami.imp;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.json.JSONObject;

import ch.zhaw.mami.RuntimeConfiguration;
import ch.zhaw.mami.Util;
import ch.zhaw.mami.db.UploadDB;

public class FolderImporter {

    private final String msmntCampaign;
    private final String format;
    private final String seq;
    private final RuntimeConfiguration runtimeConfiguration;
    private final UploadDB uploadDB;
    private final String uploader;
    private final JSONObject allMetaData;

    private final File dir;
    private final boolean locked;
    private final Path pt;
    private final SequenceFile.Writer seqWriter;
    private boolean useDb = true;

    public FolderImporter(final String lPath, final String uploader)
            throws IOException {

        dir = new File(lPath);
        if (!dir.exists()) {
            throw new IOException("Path does not exist!");
        }
        if (!dir.isDirectory()) {
            throw new IOException("Not a directory!");
        }

        this.runtimeConfiguration = RuntimeConfiguration.getInstance();
        uploadDB = runtimeConfiguration.getUploadDB();

        String allMetaPath = dir.getAbsolutePath() + File.separatorChar
                + "ALL.meta";

        File allMetaFile = new File(allMetaPath);

        if (!allMetaFile.exists()) {
            throw new RuntimeException("No ALL.meta!");
        }
        else {
            System.out.println("ALL.meta exists!");
        }

        byte[] allMetaDataBytes = Files.readAllBytes(Paths.get(allMetaPath));

        allMetaData = new JSONObject(new String(allMetaDataBytes, "UTF-8"));

        try {
            this.msmntCampaign = allMetaData.getString("msmntCampaign");
            this.seq = allMetaData.getString("seq");
            this.format = allMetaData.getString("format");
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException("Invalid metadata in ALL.meta!");
        }

        this.uploader = uploader;
        allMetaData.put("folderImport", true);

        if (!Util.validatePathPart(this.msmntCampaign)
                || !Util.validatePathPart(this.format)
                || !Util.validatePathPart(this.seq)) {
            throw new RuntimeException("Invalid msmntCampaign, format or seq!");
        }

        pt = new org.apache.hadoop.fs.Path(RuntimeConfiguration.getInstance()
                .getPathPrefix()
                + this.msmntCampaign
                + "/"
                + this.format
                + "/"
                + this.seq + ".seq");

        locked = uploadDB.getLock(pt.toString());
        if (!locked) {
            throw new RuntimeException("Could not get lock.");
        }

        seqWriter = SequenceFile.createWriter(
                runtimeConfiguration.getFSConfiguration(),
                SequenceFile.Writer.compression(CompressionType.RECORD),
                SequenceFile.Writer.keyClass(BytesWritable.class),
                SequenceFile.Writer.valueClass(BytesWritable.class),
                SequenceFile.Writer.appendIfExists(true),
                SequenceFile.Writer.file(pt));
    }

    public void close() throws IOException {
        if (locked) {
            uploadDB.releaseLock(pt.toString());
        }
        seqWriter.hflush();
        seqWriter.hsync();
        seqWriter.close();
    }

    public void importFiles() throws IOException, NoSuchAlgorithmException {
        System.out.println("Importing files...");
        String[] listing = dir.list();

        List<String> files = new ArrayList<String>();

        /* Enumerate all files. Skipping over *.meta files. */
        for (String l : listing) {
            if (l.endsWith(".meta")) {
                continue;
            }
            String path = dir.getAbsolutePath() + File.separatorChar + l;
            System.out.println(path);
            File f = new File(path);
            if (f.isFile()) {
                if (!Util.validateFileName(l)) {
                    throw new RuntimeException("Illegal filename: " + path);
                }
                files.add(l);
            }
        }

        for (String l : files) {

            System.out.println("Importing (db:" + useDb + ") " + l + "...");

            if (useDb) {
                if (uploadDB.seqUploadExists(pt.toString(), l)) {
                    throw new RuntimeException("Error: " + l
                            + " already has an upload entry!");
                }
            }

            File metaFile = new File(dir.getAbsolutePath() + File.separatorChar
                    + l + ".meta");

            JSONObject metaData = allMetaData;

            if (metaFile.exists() && metaFile.isFile()) {

                byte[] metaDataBytes = Files.readAllBytes(Paths.get(dir
                        .getAbsolutePath() + File.separatorChar + l + ".meta"));

                metaData = new JSONObject(new String(metaDataBytes, "UTF-8"));
                metaData.put("msmntCampaign", msmntCampaign);
                metaData.put("format", format);
                metaData.put("seq", seq);
                metaData.put("folderImport", true);

            }

            if (useDb) {
                uploadDB.insertSeqUpload(pt.toString(), metaData.toString(), l,
                        uploader);
            }

            byte[] data = Files.readAllBytes(Paths.get(dir.getAbsolutePath()
                    + File.separatorChar + l));
            byte[] keyBytes = l.getBytes("UTF-8");

            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(data);
            String sha1 = Util.byteArr2HexStr(md.digest());

            BytesWritable key = new BytesWritable(keyBytes);
            BytesWritable val = new BytesWritable(data);

            seqWriter.append(key, val);
            // seqWriter.hflush();
            // seqWriter.hsync();
            if (useDb) {
                uploadDB.completeSeqUpload(pt.toString(), l, sha1);
            }

        }

        seqWriter.hflush();
        seqWriter.hsync();

        System.out.println("Done.");
    }

    public void setUseDb(final boolean useDb) {
        this.useDb = useDb;
    }
}
