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

package com.hwn.bd25.sparkclickhouse;

import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hwn.bd25.sparkclickhouse.rate.Candle;
import com.hwn.bd25.sparkclickhouse.rate.MaOut;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkClickhouseSourceJob {

	public static void main(String[] args) throws Exception {
		try {
			String latestMacdTsSecondStr = null;
			String executionIdStr = null;
			if (args.length != 2) {
				return;
			} else {
				executionIdStr = args[0];
				latestMacdTsSecondStr = args[1];
			}
			Long latestMacdTs = Integer.valueOf(latestMacdTsSecondStr) * 1000L;
			Long threshold = latestMacdTs - 1 * 60 * 1000;
			String executionId = executionIdStr;
			
			System.out.println(new Date() + " >> SparkClickhouseSourceJob 001: " + executionId + ", " + latestMacdTsSecondStr);
			System.out.println(new Date() + " >> SparkClickhouseSourceJob 002");
			SparkSession spark = SparkSession.builder().appName("ClickhouseSourceCatalog")
					.config("spark.cores.max", "1")
					.config("spark.sql.catalog.clickhouse1", "com.clickhouse.spark.ClickHouseCatalog")
					.config("spark.sql.catalog.clickhouse1.host", "rc1b-aaaaaaaaabbbbbbbbbbbdddddddddd.mdb.yandexcloud.net")
					.config("spark.sql.catalog.clickhouse1.protocol", "https")
					.config("spark.sql.catalog.clickhouse1.http_port", "8443")
					.config("spark.sql.catalog.clickhouse1.user", "bigdata25")
					.config("spark.sql.catalog.clickhouse1.password", "aaaaaaaabbbbbbbbbeeeeeeeeeee")
					.config("spark.sql.catalog.clickhouse1.database", "bigdata25")
					.config("spark.sql.catalog.clickhouse1.option.ssl", "true")
					.getOrCreate();
			System.out.println(new Date() + " >> SparkClickhouseSourceJob 003");
//			Dataset<Row> ticks = spark.sql("SELECT ts, symbol, price FROM bigdata25.rates_bronze where ts > " + threshold + " and ts < " + System.currentTimeMillis());
			Dataset<Row> ticks = spark
					.sql("select symbol, CAST((ts / 60 / 1000) AS bigint) * 60 * 1000 as ts1, max(price) as price " + "from clickhouse1.bigdata25.ma where ts > "
							+ threshold + " and ts < " + System.currentTimeMillis() + " group by symbol, CAST((ts / 60 / 1000) AS bigint) * 60 * 1000");
			System.out.println(new Date() + " >> SparkClickhouseSourceJob 004");

//			List<Row> rows = ticks.repartition(2).collectAsList();
			List<Row> rows = ticks.coalesce(2).collectAsList();
			System.out.println(new Date() + " >> SparkClickhouseSourceJob 005, rows.size: " + rows.size());
			
			Map<String, Map<Long, MaOut>> ma5sIn = new HashMap();
			
//			ticks.coalesce(2).foreach((ForeachFunction<Row>) tick -> {
			for (Row tick : rows) {
				long ts = tick.getLong(tick.fieldIndex("ts1"));
				String symbol = tick.getString(tick.fieldIndex("symbol"));
				Double price = tick.getDecimal(tick.fieldIndex("price")).doubleValue();

				if (!ma5sIn.containsKey(symbol)) {
					ma5sIn.put(symbol, new HashMap());
				}
				long ts0 = ts / 1000 / 60 * 1000 * 60;
				ma5sIn.get(symbol).put(ts0, new MaOut(new Date(ts0).toString(), symbol, ts0, price));
//			});
			}

			System.out.println(new Date() + " >> SparkClickhouseSourceJob 006, ma5sIn.size: " + ma5sIn.size());
			
			List<MaOut> ma5sOut = new LinkedList();
			for (String symbol : ma5sIn.keySet()) {
				List<MaOut> sc = ma5sIn.get(symbol).values().stream()
						.sorted(Comparator.comparing(MaOut::getTs))
						.collect(Collectors.toList());
				System.out.println(new Date() + " >>   SparkClickhouseSourceJob 007, symbol: " + symbol + ", sc.size(): " + sc.size());
				ma5sOut.addAll(sc);
			}
			
			System.out.println(new Date() + " >> SparkClickhouseSourceJob 008, ma5sOut.size() " + ma5sOut.size());
			
			upload(executionId, ma5sOut);

			System.out.println(new Date() + " >> SparkClickhouseSourceJob 009, uploaded");

			spark.stop();
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}
	
	private static void upload(String executionId, List<MaOut> ma10s) throws JsonProcessingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		Files.writeString(Path.of("/tmp/" + executionId + "_ma5s.json"), mapper.writeValueAsString(ma10s));
		
		try (S3Client s3 = S3Client.create()) {

			// Build the request
			PutObjectRequest putRequest = PutObjectRequest.builder().bucket("bigdata25-rates-glue").key("airflow/" + executionId + "_ma5s.json").build();

			// Perform the upload using RequestBody.fromFile
			s3.putObject(putRequest, RequestBody.fromFile(Path.of("/tmp/" + executionId + "_ma5s.json")));

			System.out.println("Upload successful.");
		}
	}
	
//	private static void upload(String executionId, List<MaOut> ma10s) throws JsonProcessingException, IOException {
//		ObjectMapper mapper = new ObjectMapper();
//		Files.writeString(Path.of("/tmp/" + executionId + ".json"), mapper.writeValueAsString(ma10s));
//		
//		// 1. Create the Transfer Manager instance
//		S3TransferManager transferManager = S3TransferManager.create();
//
//		// 2. Build the upload request
//		UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
//		    .putObjectRequest(req -> req.bucket("bigdata25-rates-glue").key("airflow/" + executionId + ".json"))
//		    .source(Path.of("/tmp/" + executionId + ".json"))
//		    .build();
//
//		// 3. Initiate the transfer
//		FileUpload upload = transferManager.uploadFile(uploadFileRequest);
//
//		// 4. Wait for completion (the API is asynchronous by default)
//		upload.completionFuture().join();
//	}
	
	public static class MyFilterFunction implements FilterFunction<String> {

		private String pattern;
		
		public MyFilterFunction(String pattern) {
			this.pattern = pattern;
		}
		
		@Override
		public boolean call(String value) throws Exception {
			return value.contains(pattern);
		}
		
	}

}