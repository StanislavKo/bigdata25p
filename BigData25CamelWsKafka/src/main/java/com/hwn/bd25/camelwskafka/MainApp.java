package com.hwn.bd25.camelwskafka;

import org.apache.camel.main.Main;
import org.apache.camel.support.SimpleRegistry;
import org.apache.camel.support.jsse.SSLContextParameters;

/**
 * A Camel Application
 */
public class MainApp {

	/**
	 * A main() so we can easily run these routing rules in our IDE
	 */
	public static void main(String... args) throws Exception {
//        SimpleRegistry registry = new SimpleRegistry();
//
//        // Minimal SSL config for WSS (default truststore is enough)
		SSLContextParameters ssl = new SSLContextParameters();
//        registry.bind("mySsl", ssl);
//
//        CamelContext context = new DefaultCamelContext(registry);

		Main main = new Main();
		main.bind("mySsl", ssl);
		main.bind("myPath", "?token=AAAAAAAABBBBBBBBBBCCCCCCC");
		main.configure().addRoutesBuilder(new MyRouteBuilder());
		main.run(args);
	}

}
