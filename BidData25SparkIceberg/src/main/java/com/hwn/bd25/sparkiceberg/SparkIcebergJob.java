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

package com.hwn.bd25.sparkiceberg;

import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hwn.bd25.sparkiceberg.rate.Candle;
import com.hwn.bd25.sparkiceberg.rate.MaOut;

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

public class SparkIcebergJob {

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
			Long threshold = latestMacdTs - 10 * 60 * 1000;
			String executionId = executionIdStr;
			
			System.out.println(new Date() + " >> SparkIcebergJob 001: " + executionId + ", " + latestMacdTsSecondStr);
			System.out.println(new Date() + " >> SparkIcebergJob 002");
			SparkSession spark = SparkSession.builder().appName("IcebergGlueCatalog")
					.config("spark.cores.max", "1")
					.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
					.config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
					.config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
					.config("spark.sql.catalog.glue_catalog.warehouse", "s3a://bigdata25-rates-glue/bigdata25/rates-bronze3")
					// ... other configurations ...
//					.config("spark.hadoop.fs.s3a.access.key", "AAAAAABBBBBBFFFFFFFFF")
//					.config("spark.hadoop.fs.s3a.secret.key", "AAAAAAAAAAAAAABBBBBBBBBBBBBBGGGGGGGGGGGGGGGG")
//					.config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
//					.config("spark.hadoop.fs.s3a.endpoint.region", "ap-northeast-2")
					// Optional: Set the region if not defined by default credentials
					.config("spark.sql.catalog.glue_catalog.aws.region", "ap-northeast-2")
					// Make sure to add the S3FileIO implementation property
					.config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
					.config("spark.sql.defaultCatalog", "glue_catalog")
					.config("spark.sql.adaptive.enabled", "false")
					.getOrCreate();
			System.out.println(new Date() + " >> SparkIcebergJob 003");
//			Dataset<Row> ticks = spark.sql("SELECT ts, symbol, price FROM bigdata25.rates_bronze where ts > " + threshold + " and ts < " + System.currentTimeMillis());
			Dataset<Row> ticks = spark
					.sql("select symbol, CAST((ts / 60 / 1000) AS BIGINT) * 60 * 1000 as ts, max(price) as price " + "from bigdata25.rates_bronze3 where ts > "
							+ threshold + " and ts < " + System.currentTimeMillis() + " group by symbol, CAST((ts / 60 / 1000) AS BIGINT) * 60 * 1000");
			System.out.println(new Date() + " >> SparkIcebergJob 004");

//			List<Row> rows = ticks.repartition(2).collectAsList();
			List<Row> rows = ticks.coalesce(2).collectAsList();
			System.out.println(new Date() + " >> SparkIcebergJob 005, rows.size: " + rows.size());
			
			Map<String, Map<Long, Candle>> candles = new HashMap();
			
//			ticks.coalesce(2).foreach((ForeachFunction<Row>) tick -> {
			for (Row tick : rows) {
				long ts = tick.getLong(tick.fieldIndex("ts"));
				String symbol = tick.getString(tick.fieldIndex("symbol"));
				Double price = Double.valueOf(tick.getFloat(tick.fieldIndex("price")));

				if (!candles.containsKey(symbol)) {
					candles.put(symbol, new HashMap());
				}
				long ts0 = ts / 1000 / 60 * 1000 * 60;
				if (!candles.get(symbol).containsKey(ts0)) {
					candles.get(symbol).put(ts0, new Candle(symbol, ts0, 1000 * 60, price, price));
				} else {
					Candle candle = candles.get(symbol).get(ts0);
					if (candle.getMinPrice() > price) {
						candle.setMinPrice(price);
					}
					if (candle.getMaxPrice() < price) {
						candle.setMaxPrice(price);
					}
				}
				
//				// Perform operations on the row data
//				System.out.println("Row data: " + columnNameValue);
//			});
			}

			System.out.println(new Date() + " >> SparkIcebergJob 006, candles.size: " + candles.size());
			
			List<MaOut> ma10s = new LinkedList();
			for (String symbol : candles.keySet()) {
				List<Candle> sc = candles.get(symbol).values().stream()
						.sorted(Comparator.comparing(Candle::getTs))
						.collect(Collectors.toList());
				if (sc.size() < 10) {
					continue;
				}
				System.out.println(new Date() + " >>   SparkIcebergJob 007, symbol: " + symbol + ", sc.size(): " + sc.size());
				for (int i = 10; i < sc.size(); i++) {
					Double ma10 = sc.get(i - 9).getMaxPrice() / 512 + sc.get(i - 8).getMaxPrice() / 512 + sc.get(i - 7).getMaxPrice() / 256
							+ sc.get(i - 6).getMaxPrice() / 128 + sc.get(i - 5).getMaxPrice() / 64 + sc.get(i - 4).getMaxPrice() / 32
							+ sc.get(i - 3).getMaxPrice() / 16 + sc.get(i - 2).getMaxPrice() / 8 + sc.get(i - 1).getMaxPrice() / 4
							+ sc.get(i).getMaxPrice() / 2;
					ma10s.add(new MaOut(new Date(sc.get(i).getTs()).toString(), symbol, sc.get(i).getTs(), ma10));
				}
			}
			
			System.out.println(new Date() + " >> SparkIcebergJob 008, ma10s.size() " + ma10s.size());
			
			upload(executionId, ma10s);

			System.out.println(new Date() + " >> SparkIcebergJob 009, uploaded");

			spark.stop();
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}
	
	private static void upload(String executionId, List<MaOut> ma10s) throws JsonProcessingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		Files.writeString(Path.of("/tmp/" + executionId + "_ma10s.json"), mapper.writeValueAsString(ma10s));
		
		try (S3Client s3 = S3Client.create()) {

			// Build the request
			PutObjectRequest putRequest = PutObjectRequest.builder().bucket("bigdata25-rates-glue").key("airflow/" + executionId + "_ma10s.json").build();

			// Perform the upload using RequestBody.fromFile
			s3.putObject(putRequest, RequestBody.fromFile(Path.of("/tmp/" + executionId + "_ma10s.json")));

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