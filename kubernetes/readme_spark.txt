curl -X POST -H 'Content-Type: application/json' --data '{
  "action": "CreateSubmissionRequest",
  "clientSparkVersion": "3.5.0",
  "appResource": "hdfs://your-hdfs-path/your-application.jar",
  "mainClass": "com.example.SparkPi",
  "environmentVariables": {
    "SPARK_ENV_LOADED": "1"
  },
  "supervise": false,
  "appArgs": [
    "100"
  ],
  "sparkProperties": {
    "spark.driver.supervise": "false",
    "spark.app.name": "SparkPi",
    "spark.eventLog.enabled": "false",
    "spark.submit.deployMode": "cluster",
    "spark.master": "spark://your-spark-master:7077"
  }
}' http://your-spark-master:6066/v1/submissions/create
