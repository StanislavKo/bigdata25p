/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hwn.bd25.flinkkafkama;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hwn.bd25.flinkkafkama.rate.MaOut;
import com.hwn.bd25.flinkkafkama.rate.RateIn;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
//import java.util.logging.LogManager;
import java.util.stream.Collectors;
import java.util.Date;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

/**
 * This is a re-write of the Apache Flink WordCount example using Kafka connectors.
 * Find the original example at 
 * https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/wordcount/WordCount.java
 */
public class FlinkKafkaMaJobSimple {

	final static String inputTopic = "ws_raw";
	final static String outputTopic = "ma";
	final static String group = "flink-ma" +  + System.currentTimeMillis();
	final static String jobTitle = "FlinkKafkaMa_" + System.currentTimeMillis();
	
//	private static Logger logger = LogManager.getLogger(FlinkKafkaMaJob.class); 

	public static void main(String[] args) throws Exception {
		final ObjectMapper mapper = new ObjectMapper();
		
		final String bootstrapServers = args.length > 0 ? args[0] : "kafka-broker-1:9092,kafka-broker-2:9093,kafka-broker-3:9094";

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> source = KafkaSource.<String>builder()
		    .setBootstrapServers(bootstrapServers)
		    .setTopics(inputTopic)
		    .setGroupId(group)
		    .setStartingOffsets(OffsetsInitializer.latest())
		    .setValueOnlyDeserializer(new SimpleStringSchema())
		    .build();

		KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
			.setValueSerializationSchema(new SimpleStringSchema())
			.setTopic(outputTopic)
			.build();

		KafkaSink<String> sink = KafkaSink.<String>builder()
			.setBootstrapServers(bootstrapServers)
			.setRecordSerializer(serializer)
			.build();

		DataStream<String> symbolRateStream = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");


		// Split up the lines in pairs (2-tuples) containing: (word,1)
		DataStream<String> mas = symbolRateStream
				.keyBy(new MyKeySelector())
				.process(new CountWithTimeoutFunction());

		// Add the sink to so results
		// are written to the outputTopic
		mas.sinkTo(sink);

		// Execute program
		env.execute(jobTitle);
	}

	public static class CountWithTimeoutFunction extends KeyedProcessFunction<String, String, String> {

		private ValueState<AtomicInteger> state;

		@Override
		public void open(OpenContext openContext) throws Exception {
			state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", AtomicInteger.class));
		}

		@Override
		public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
			// retrieve the current count
			AtomicInteger current = state.value();
			if (current == null) {
				current = new AtomicInteger();
				current.set(0);
			}

			// update the state's count
			current.addAndGet(1);

			// write the state back
			state.update(current);

			// schedule the next timer 60 seconds from the current event time
			ctx.timerService().registerEventTimeTimer(System.currentTimeMillis() + 15000);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

			// get the state for the key that scheduled the timer
			AtomicInteger result = state.value();

			// check if this is an outdated timer or the latest timer
//			if (timestamp == result.lastModified + 60000) {
				// emit the state on timeout
			out.collect("{\"symbol\":\"BINANCE:BTCUSDT\",\"ts\":1764785534502,\"price\":" + result.get() + "}");
//			}
		}
	}

	public static final class MyKeySelector implements KeySelector<String, String> {

		private final ObjectMapper mapper = new ObjectMapper();

		@Override
		public String getKey(String value) throws Exception {
			RateIn rate = mapper.readValue(value, RateIn.class);
			return rate.getSymbol();
		}
	}

}