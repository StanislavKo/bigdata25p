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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hwn.bd25.kafkaconnectwssource.WsSourceTask.WS_URI_FIELD;
import static com.hwn.bd25.kafkaconnectwssource.WsSourceTask.POSITION_FIELD;

/**
 * Very simple source connector that works with web-socket.
 */
public class WsSourceConnector extends SourceConnector {

	private static final Logger log = LoggerFactory.getLogger(WsSourceConnector.class);
	public static final String TOPIC_CONFIG = "topic";
	public static final String WS_URI_CONFIG = "ws_uri";
//	public static final String WS_INIT_MESSAGES = "ws_init_messages";

	static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(WS_URI_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), Importance.HIGH,
					"Source filename. If not specified, the standard input will be used")
//			.define(WS_INIT_MESSAGES, Type.LIST, null, Importance.LOW, "Messages to send into web-socket at onOpen event")
			.define(TOPIC_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), Importance.HIGH, "The topic to publish data to")
			;

	private Map<String, String> props;

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		this.props = props;
		AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
		String wsUri = config.getString(WS_URI_CONFIG);
//		List<String> wsInitMessages = config.getList(WS_INIT_MESSAGES);
		log.info("Starting web-socket source connector reading from {}", wsUri);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return WsSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		// Only one input stream makes sense.
		configs.add(props);
		return configs;
	}

	@Override
	public void stop() {
		// Nothing to do since FileStreamSourceConnector has no background monitoring.
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
		return ExactlyOnceSupport.UNSUPPORTED;
	}

	@Override
	public boolean alterOffsets(Map<String, String> connectorConfig, Map<Map<String, ?>, Map<String, ?>> offsets) {
		throw new ConnectException("Offsets cannot be modified for web-socket configuration is unspecified.");
	}
}