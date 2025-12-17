package com.hwn.bd25.kafkaconnectwssource;

import java.util.List;
import java.util.Queue;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Listener;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hwn.bd25.kafkaconnectwssource.rate.Packet;
import com.hwn.bd25.kafkaconnectwssource.rate.RateIn;
import com.hwn.bd25.kafkaconnectwssource.rate.RateOut;

public class WebSocketClient implements WebSocket.Listener {

	private WebSocket webSocket;
	private final String wsUri;
	private final CountDownLatch cdl;
	private final Queue<String> messages;
	private final List<String> openCommands;
	private final ObjectMapper mapper;

	public WebSocketClient(String wsUri, CountDownLatch cdl, Queue<String> messages, List<String> openCommands) {
		this.wsUri = wsUri;
		this.cdl = cdl;
		this.messages = messages;
		this.openCommands = openCommands;
		
		mapper = new ObjectMapper();

		WebSocket ws = HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(URI.create(wsUri), this).join();
//		ws.sendText("Hello!", true);
	}

	@Override
	public void onOpen(WebSocket webSocket) {
		this.webSocket = webSocket;
		System.out.println("onOpen using subprotocol " + webSocket.getSubprotocol());
		WebSocket.Listener.super.onOpen(webSocket);
		for (String openCommand : openCommands) {
			System.out.println("onOpen send command >> " + openCommand);
			webSocket.sendText(openCommand, true);
		}
	}

	@Override
	public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
		System.out.println("onPing received");
		return Listener.super.onPing(webSocket, message);
	}

	@Override
	public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
		System.out.println("onPong sent");
		return Listener.super.onPong(webSocket, message);
	}

	@Override
	public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
		System.out.println("onText received " + data);
//		latch.countDown();
		try {
			if ("{\"type\":\"ping\"}".equals(data.toString())) {
				webSocket.sendText("{\"type\":\"pong\"}", true);
			}
		} catch (Exception ignored) {
		}
		try {
			Packet packet = mapper.readValue(data.toString(), Packet.class);
			synchronized (messages) {
				for (RateIn rateIn : packet.getData()) {
					RateOut rate = new RateOut(rateIn.getS(), rateIn.getT(), rateIn.getP(), rateIn.getV());
					messages.add(mapper.writeValueAsString(rate));
				}
			}
		} catch (Exception ignored) {
		}
		return WebSocket.Listener.super.onText(webSocket, data, last);
	}

	@Override
	public void onError(WebSocket webSocket, Throwable error) {
		System.out.println("Bad day! " + webSocket.toString());
		cdl.countDown();
		WebSocket.Listener.super.onError(webSocket, error);
	}

	public void abort() {
		webSocket.abort();
	}

}
