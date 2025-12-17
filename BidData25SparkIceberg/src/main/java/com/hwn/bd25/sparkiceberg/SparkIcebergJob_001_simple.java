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
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import java.util.Date;

public class SparkIcebergJob_001_simple {

	public static void main(String[] args) throws Exception {
		try {
			System.out.println(new Date() + " >> SparkIcebergJob 001: " + (args.length > 0 ? args[0] : "empty"));
			String logFile = "/opt/spark/README.md"; // Should be some file on your system
			System.out.println(new Date() + " >> SparkIcebergJob 002");
			SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.cores.max", "1").getOrCreate();
			System.out.println(new Date() + " >> SparkIcebergJob 003");
			Dataset<String> logData = spark.read().textFile(logFile).cache();
			System.out.println(new Date() + " >> SparkIcebergJob 004");

			long numAs = logData.filter(new MyFilterFunction("a")).count();
			long numBs = logData.filter(new MyFilterFunction("b")).count();

			System.out.println(new Date() + " >> Lines with a: " + numAs + ", lines with b: " + numBs);

			spark.stop();
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}
	
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