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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
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
public class FlinkKafkaMaJob {

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
				.window(SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofSeconds(15)))
//				.window(TumblingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofSeconds(15)))
				.process(new MyProcessWindowFunction());

		// Add the sink to so results
		// are written to the outputTopic
		mas.sinkTo(sink);

		// Execute program
		env.execute(jobTitle);
	}

	public static final class MyKeySelector implements KeySelector<String, String> {

		private final ObjectMapper mapper = new ObjectMapper();

		@Override
		public String getKey(String value) throws Exception {
			RateIn rate = mapper.readValue(value, RateIn.class);
			return rate.getSymbol();
		}
	}

	public static final class MyProcessWindowFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {

		private final ObjectMapper mapper = new ObjectMapper();

		@Override
		public void process(String key, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> out)
				throws Exception {
			System.out.println("################# " + key + " " + (new Date()));
			try {
				Long now = System.currentTimeMillis();

				List<RateIn> rates = new LinkedList();
				for (String item : iterable) {
					RateIn rate = mapper.readValue(item, RateIn.class);
					rates.add(rate);
				}
				rates = rates.stream().sorted(Comparator.comparing(RateIn::getTs)).collect(Collectors.toList());
				
				double[] prices = new double[6];
				prices[0] = rates.get(0).getPrice();
				prices[5] = rates.get(rates.size() - 1).getPrice();
				for (int i = 4; i >= 1; i--) {
					Long nowMinusXminutes = now - i * 60 * 1000;
					for (RateIn rate : rates) {
						if (rate.getTs() >= nowMinusXminutes) {
							prices[5 -i] = rate.getPrice();
							break;
						}
					}
				}
				double ma = prices[0]/32 + prices[1]/32 + prices[2]/16 + prices[3]/8 + prices[4]/4 + prices[5]/2;
				
				String timeStr = Instant.ofEpochMilli(now).toString();
				timeStr = timeStr.substring(0, timeStr.length() - 1);
				timeStr = timeStr.replace("T", " ");
				MaOut result = new MaOut(timeStr, key, now, ma);
				String resultStr = mapper.writeValueAsString(result);
				
				out.collect(resultStr);
			} catch (Exception ex) {
				ex.printStackTrace();
//				logger.error("Can't do", ex);
				out.collect("{\"symbol\":\"BINANCE:BTCUSDT\",\"ts\":1764785534502,\"price\":46585.666249999995}");
			}
		}

	}

	public static final class MyReduceFunction implements ReduceFunction<String> {

		private final ObjectMapper mapper = new ObjectMapper();

		@Override
		public String reduce(String value1, String value2) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

	}

	// Implements a simple reducer using FlatMap to
	// reduce the Tuple2 into a single string for
	// writing to kafka topics
	public static final class Reducer implements FlatMapFunction<Tuple2<String, Integer>, String> {

		@Override
		public void flatMap(Tuple2<String, Integer> value, Collector<String> out) {
			// Convert the pairs to a string
			// for easy writing to Kafka Topic
			String count = value.f0 + " " + value.f1;
			out.collect(count);
		}
	}
}