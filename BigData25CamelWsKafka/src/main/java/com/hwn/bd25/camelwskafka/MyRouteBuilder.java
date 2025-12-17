package com.hwn.bd25.camelwskafka;

import org.apache.camel.Endpoint;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.FileEndpoint;
//import org.apache.camel.component.websocket.WebsocketComponent;
//import org.apache.camel.component.websocket.WebsocketEndpoint;
import org.apache.camel.support.jsse.SSLContextParameters;

/**
 * A Camel Java DSL Router
 */
public class MyRouteBuilder extends RouteBuilder {

	/**
	 * Let's configure the Camel routing rules using Java code...
	 * 
	 * @throws Exception
	 */
	public void configure() throws Exception {

		// here is a sample which processes the input files
		// (leaving them in place - see the 'noop' flag)
		// then performs content based routing on the message using XPath
//        from("file:src/data?noop=true")
//            .choice()
//                .when(xpath("/person/city = 'London'"))
//                    .log("UK message")
//                    .to("file:target/messages/uk")
//                .otherwise()
//                    .log("Other message")
//                    .to("file:target/messages/others");
//		WebsocketComponent wsComponent = new WebsocketComponent();
////		Endpoint fromWs = wsComponent.createEndpoint("wss://ws.finnhub.io?token=AAAAAAAABBBBBBBBBBCCCCCCC");
//		WebsocketEndpoint wsEndpoint = new WebsocketEndpoint(wsComponent, "wss://ws.finnhub.io?token=AAAAAAAABBBBBBBBBBCCCCCCC", null);
////		WebsocketConsumer wsConsumer = wsEndpoint.
//		from(wsEndpoint).log("asdf").to("file:target/messages/others");

//		from("ahc-ws:wss://ws.finnhub.io/?bridgeEndpoint=true&throwExceptionOnFailure=false&ahcWsEndpointUri=wss://ws.finnhub.io/?token=AAAAAAAABBBBBBBBBBCCCCCCC")
//			.choice()
//				.when(header("websocket.event").isEqualTo("ONOPEN"))
//					.setBody().constant("{\"type\":\"subscribe\",\"symbol\":\"BINANCE:BTCUSDT\"}")
//					.to("ahc-ws:wss://ws.finnhub.io/?bridgeEndpoint=true&throwExceptionOnFailure=false&ahcWsEndpointUri=wss://ws.finnhub.io/?token=AAAAAAAABBBBBBBBBBCCCCCCC")
//				.when(header("websocket.event").isNull())
//					.log("Received message from client: ${body}")
//				.when(header("websocket.event").isEqualTo("ONCLOSE"))
//					.log("WebSocket connection closed: ${header.websocket.connectionKey}")
//			.end();

//		from("ahc-wss:ws.finnhub.io/?token.raw=AAAAAAAABBBBBBBBBBCCCCCCC")
//			.log("FInnhub message: ${body}");
		
//		from("undertow:wss://ws.finnhub.io?token.raw=AAAAAAAABBBBBBBBBBCCCCCCC")
//			.routeId("finnhub-ws-client")
//			.log("WebSocket connected or message received: ${body}")
//			.to("direct:process");
//
//      from("direct:process")
//          .choice()
//              .when(header("CamelUndertowWebSocketEvent").isEqualTo("ONOPEN"))
//                  .setBody().constant("{\"type\":\"subscribe\",\"symbol\":\"AAPL\"}")
//                  .log("Sending subscription: ${body}")
//                  .to("undertow:wss://ws.finnhub.io?token.raw=AAAAAAAABBBBBBBBBBCCCCCCC")
//              .otherwise()
//                  .log("Market data event: ${body}")
//          .end();
		
//		from("vertx-websocket:ws.finnhub.io?consumeAsClient=true&sslContextParameters=wss")
//			.log("Got WebSocket message ${body}");
		
//		from("vertx-http-ws-client:ws.finnhub.io:443/?useSsl=true&path.raw=/?token=AAAAAAAABBBBBBBBBBCCCCCCC")
//			.routeId("finnhub-vertx-ws-client")
//			.log("Received FINNHUB message: ${body}")
//			.to("direct:subscribe-handler");
//		
//		from("direct:subscribe-handler")
//        .choice()
//            .when(header("CamelVertxHttpWebSocketEvent").isEqualTo("CONNECTED"))
//                // Subscribe once WS is open
//                .setBody().constant("{\"type\":\"subscribe\",\"symbol\":\"AAPL\"}")
//                .log("Sending subscription: ${body}")
//                .to("vertx-http-ws-client:ws.finnhub.io:443/?useSsl=true&path.raw=/?token=AAAAAAAABBBBBBBBBBCCCCCCC")
//            .otherwise()
//                .log("Tick message: ${body}")
//        .end();

//		from("vertx-websocket:ws.finnhub.io:443/?"
//                + "consumeAsClient=true"
//                + "&ssl=true"
//                + "&sslContextParameters=#mySsl"
//                + "&host=ws.finnhub.io"
//                + "&port=443"
//                + "&path.raw=/?token=AAAAAAAABBBBBBBBBBCCCCCCC")
//		.log("Got WebSocket message: ${body}");
	}

}
