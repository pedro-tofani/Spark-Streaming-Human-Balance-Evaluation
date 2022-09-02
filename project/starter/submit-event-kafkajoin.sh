#!/bin/bash
CONTAINER_NAME=spark-streaming-human-balance-evaluation_spark_1
docker exec -it $CONTAINER_NAME /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /home/workspace/project/starter/sparkpykafkajoin.py | tee ../../spark/logs/kafkajoin.log 