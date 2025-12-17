package com.hwn.bd25.kafkaconnectwssource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WebSocketClientTest {

	private WebSocketClient wsClient;
	private Queue<String> messages;

	@BeforeEach
	public void setUp() {
		CountDownLatch cdl = new CountDownLatch(1);
		messages = new LinkedBlockingQueue<>(100);
		wsClient = new WebSocketClient("wss://ws.finnhub.io?token=AAAAAAAABBBBBBBBBBCCCCCCC", cdl, messages,
				List.of("{\"type\":\"subscribe\",\"symbol\":\"BINANCE:BTCUSDT\"}"));
	}
	
	@Test
	public void simpleTest() throws InterruptedException {
		Thread.sleep(6_000);
		assertTrue(!messages.isEmpty());
	}

}
