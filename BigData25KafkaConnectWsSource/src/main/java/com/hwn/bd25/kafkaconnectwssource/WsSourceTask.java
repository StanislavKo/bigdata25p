/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hwn.bd25.kafkaconnectwssource;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * FileStreamSourceTask reads from stdin or a file.
 */
public class WsSourceTask extends SourceTask {
	private static final Logger log = LoggerFactory.getLogger(WsSourceTask.class);
	public static final String WS_URI_FIELD = "ws_uri";
	public static final String WS_INIT_MESSAGES_FIELD = "ws_init_messages";
	public static final String POSITION_FIELD = "position";
	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
	private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

	private String wsUri;
	private List<String> wsInitMessages;
//	private InputStream stream;
//	private BufferedReader reader = null;
//	private char[] buffer;
	private Queue<String> messages;
	private CountDownLatch cdl;
	private WebSocketClient wsClient;
	private int offset = 0;
	private String topic;

	private Long streamOffset = null;

	public WsSourceTask() {
//		this(1024);
		messages = new LinkedBlockingQueue<>(100);
	}

//	/* visible for testing */
//	WsSourceTask(int initialBufferSize) {
////		buffer = new char[initialBufferSize];
//	}

	@Override
	public String version() {
		return new WsSourceConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {
		AbstractConfig config = new AbstractConfig(WsSourceConnector.CONFIG_DEF, props);
		wsUri = config.getString(WsSourceConnector.WS_URI_CONFIG);
		topic = config.getString(WsSourceConnector.TOPIC_CONFIG);
//		wsInitMessages = config.getList(WsSourceConnector.WS_INIT_MESSAGES);
		wsInitMessages = List.of("{\"type\":\"subscribe\",\"symbol\":\"BINANCE:BTCUSDT\"}", "{\"type\":\"subscribe\",\"symbol\":\"BINANCE:ETHUSDT\"}");

		cdl = new CountDownLatch(1);
		wsClient = new WebSocketClient(wsUri, cdl, messages, wsInitMessages);
		new Thread(new WsClientMonitor()).start();
	}

	public class WsClientMonitor implements Runnable {
		@Override
		public void run() {
			try {
				cdl.await();

				cdl = new CountDownLatch(1);
				wsClient = new WebSocketClient(wsUri, cdl, messages, wsInitMessages);
				new Thread(new WsClientMonitor()).start();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		LinkedList<SourceRecord> records = new LinkedList();

		synchronized (messages) {
			while (!messages.isEmpty()) {
				try {
					records.add(new SourceRecord(offsetKey(wsUri), offsetValue(streamOffset), topic, null, KEY_SCHEMA, UUID.randomUUID().toString(),
							VALUE_SCHEMA, messages.poll(), System.currentTimeMillis()));
				} catch (Exception ex) {
					ex.printStackTrace();
				}

				if (records.size() >= 20) {
					return records;
				}
			}
		}
		return records;
	}

	@Override
	public void stop() {
		log.trace("Stopping");
		synchronized (this) {
			wsClient.abort();
			this.notify();
		}
	}

	private Map<String, String> offsetKey(String wsUri) {
		return Collections.singletonMap(WS_URI_FIELD, wsUri);
	}

	private Map<String, Long> offsetValue(Long pos) {
		return Collections.singletonMap(POSITION_FIELD, pos);
	}

}