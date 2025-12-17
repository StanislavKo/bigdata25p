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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hwn.bd25.sparkclickhouse.rate.Candle;
import com.hwn.bd25.sparkclickhouse.rate.MaOut;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import org.apache.hadoop.thirdparty.com.google.common.base.Functions;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkClickhouseSinkJob {

	public static void main(String[] args) throws Exception {
		try {
			String executionIdStr = null;
			if (args.length != 1) {
				return;
			} else {
				executionIdStr = args[0];
			}
			String executionId = executionIdStr;
			
			List<MaOut> ma5s = loadMa(executionIdStr, "_ma5s");
			List<MaOut> ma10s = loadMa(executionIdStr, "_ma10s");
			
			System.out.println(new Date() + " >> SparkClickhouseSinkJob 001: " + executionId);
			System.out.println(new Date() + " >> SparkClickhouseSinkJob 002");
			SparkSession spark = SparkSession.builder().appName("ClickhouseSourceCatalog")
					.config("spark.cores.max", "1")
					.config("spark.sql.catalog.clickhouse1", "com.clickhouse.spark.ClickHouseCatalog")
					.config("spark.sql.catalog.clickhouse1.host", "rc1b-aaaaaaaaaaabbbbbbbbdddddddd.mdb.yandexcloud.net")
					.config("spark.sql.catalog.clickhouse1.protocol", "https")
					.config("spark.sql.catalog.clickhouse1.http_port", "8443")
					.config("spark.sql.catalog.clickhouse1.user", "bigdata25")
					.config("spark.sql.catalog.clickhouse1.password", "aaaaaaaabbbbbbbbbeeeeeeeeeee")
					.config("spark.sql.catalog.clickhouse1.database", "bigdata25")
					.config("spark.sql.catalog.clickhouse1.option.ssl", "true")
					.getOrCreate();
			System.out.println(new Date() + " >> SparkClickhouseSinkJob 003");

	        // Define the schema for the DataFrame
	        StructType schema = new StructType(new StructField[]{
	                DataTypes.createStructField("date", DataTypes.DateType, false),
	                DataTypes.createStructField("time", DataTypes.TimestampType, false),
	                DataTypes.createStructField("ts", DataTypes.LongType, false),
	                DataTypes.createStructField("symbol", DataTypes.StringType, false),
	                DataTypes.createStructField("macd", DataTypes.DoubleType, false)
	        });

			Map<String, MaOut> ma5sMap = ma5s.stream().collect(Collectors.toMap(ma -> ma.getSymbol() + "_" + ma.getTs(), Functions.identity()));
			Map<String, MaOut> ma10sMap = ma10s.stream().collect(Collectors.toMap(ma -> ma.getSymbol() + "_" + ma.getTs(), Functions.identity()));
			List<Row> macd = new LinkedList();
			for (String key : ma5sMap.keySet()) {
				if (ma10sMap.containsKey(key)) {
					MaOut ma5 = ma5sMap.get(key);
					MaOut ma10 = ma10sMap.get(key);
					Instant time = Instant.ofEpochMilli(ma5.getTs());
					macd.add(RowFactory.create(java.time.LocalDate.ofInstant(time, ZoneId.of("UTC")), time, ma5.getTs(), ma5.getSymbol(), ma5.getPrice() - ma10.getPrice()));
				}
			}

//			List<Row> data = Arrays.asList(
//	                RowFactory.create(1, "Alice"),
//	                RowFactory.create(2, "Bob")
//	        );

	        // Create a DataFrame
	        Dataset<Row> df = spark.createDataFrame(macd, schema);

	        df.writeTo("clickhouse1.bigdata25.macd").append();
			
			System.out.println(new Date() + " >> SparkClickhouseSinkJob 004");

			upload(executionId);

			System.out.println(new Date() + " >> SparkClickhouseSinkJob 009, uploaded");

			spark.stop();
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}
	
	private static List<MaOut> loadMa(String executionId, String name) throws JsonProcessingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		try (S3Client s3 = S3Client.create()) {

			// Build the request
			GetObjectRequest getRequest = GetObjectRequest.builder().bucket("bigdata25-rates-glue").key("airflow/" + executionId + name + ".json").build();

			// Perform the upload using RequestBody.fromFile
			ResponseInputStream<GetObjectResponse> maObject = s3.getObject(getRequest);
			
			String maJsonStr = new String(maObject.readAllBytes(), StandardCharsets.UTF_8);

			System.out.println("maJsonStr = " + maJsonStr);
			
			return mapper.readValue(maJsonStr, new TypeReference<List<MaOut>>() {
			});
		}
	}
	
	private static void upload(String executionId) throws JsonProcessingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		Files.writeString(Path.of("/tmp/" + executionId + "_finish.json"), mapper.writeValueAsString("{}"));
		
		try (S3Client s3 = S3Client.create()) {

			// Build the request
			PutObjectRequest putRequest = PutObjectRequest.builder().bucket("bigdata25-rates-glue").key("airflow/" + executionId + "_finish.json").build();

			// Perform the upload using RequestBody.fromFile
			s3.putObject(putRequest, RequestBody.fromFile(Path.of("/tmp/" + executionId + "_finish.json")));

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