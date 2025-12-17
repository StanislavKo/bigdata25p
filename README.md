# bigdata25

![Overview](/img/architecture.png)

[Youtube](https://www.youtube.com/watch?v=7sp--pvwTXw&)

- [K8s](https://www.youtube.com/watch?v=7sp--pvwTXw&t=9s)
- [Kafka](https://www.youtube.com/watch?v=7sp--pvwTXw&t=15s)
- [Flink](https://www.youtube.com/watch?v=7sp--pvwTXw&t=29s)
- [Iceberg](https://www.youtube.com/watch?v=7sp--pvwTXw&t=37s)
- [ClickHouse](https://www.youtube.com/watch?v=7sp--pvwTXw&t=1m27s)
- [Airflow](https://www.youtube.com/watch?v=7sp--pvwTXw&t=1m53s)
- [Spark SQL](https://www.youtube.com/watch?v=7sp--pvwTXw&t=2m06s)
- [dbt](https://www.youtube.com/watch?v=7sp--pvwTXw&t=2m16s)
- [Trino](https://www.youtube.com/watch?v=7sp--pvwTXw&t=2m25s)
- [Superset](https://www.youtube.com/watch?v=7sp--pvwTXw&t=2m38s)

Some hints about setup:

- Iceberg is not reliable in this setup. So data folder in AWS is populated by writer. But metadata folder is not updated (after writer first interruption). So Spark Connector or AWS Athena don't see fresh data;
- Airflow doesn’t implement incremental MACD. Because I already have 1 Spark job for reading from ClickHouse and 1 Spark job for writing into ClickHouse. No need to create yet another reader.
