package ch.zhaw.mami;

import java.util.HashMap;
import java.util.Map;

import ch.zhaw.mami.imp.FolderImporter;

import com.sun.grizzly.http.SelectorThread;
import com.sun.jersey.api.container.grizzly.GrizzlyWebContainerFactory;

/**
 * Hello world!
 * 
 */

public class App {

    public static void folderImport(final String[] args) throws Exception {
        if (args.length != 3) {
            throw new RuntimeException("Need more arguments: <path> <uploader>");
        }
        System.out.println("Importing from: " + args[1]);
        FolderImporter fi = null;
        try {
            fi = new FolderImporter(args[1], args[2]);
            if (args[2].equals("nodb")) {
                fi.setUseDb(false);
            }
            fi.importFiles();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            if (fi != null) {
                fi.close();
            }
        }
    }

    public static void main(final String[] args) throws Exception {

        // System.setProperty("HADOOP_USER_NAME", "hdfs-mami");

        if (args.length > 0) {
            if (args[0].equals("folderImport")) {
                try {
                    App.folderImport(args);
                    System.exit(0);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.exit(1);
                }
            }
            else {
                System.out.println("Invalid cmdline arguments.");
                System.exit(1);
            }
        }

        if (System.getProperty("log4j.configurationFile") == null) {
            System.out.println("Using default /etc/hdfs-mami/logger.xml!");
            System.setProperty("log4j.configurationFile",
                    "/etc/hdfs-mami/logger.xml");
        }

        final String baseUri = RuntimeConfiguration.getInstance().getURL();
        final Map<String, String> initParams = new HashMap<String, String>();

        initParams.put("com.sun.jersey.config.property.packages",
                "ch.zhaw.mami");
        initParams.put("com.sun.jersey.api.json.POJOMappingFeature", "true");

        System.out.println("Starting grizzly...");
        SelectorThread threadSelector = GrizzlyWebContainerFactory.create(
                baseUri, initParams);
        System.out.println(String.format(
                "Jersey app started with WADL available at %sapplication.wadl\n"
                        + "Try out %shelloworld\nHit enter to stop it...",
                baseUri, baseUri));
        System.in.read();
        threadSelector.stopEndpoint();
        System.exit(0);
    }
}
