package ch.zhaw.mami;

import java.util.HashMap;
import java.util.Map;

import com.sun.grizzly.http.SelectorThread;
import com.sun.jersey.api.container.grizzly.GrizzlyWebContainerFactory;

/**
 * Hello world!
 * 
 */

public class App {

	public static void main(final String[] args) throws Exception {

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
