#!/bin/bash

rm -rf /tmp/flink*
export JAVA_HOME=/home/swrrt11/softwares/kit/jdk
~/samza-hello-samza/bin/grid stop kafka
~/samza-hello-samza/bin/grid stop zookeeper
kill -9 $(jps |grep Kafka|awk '{print $1}')
rm -rf /data/kafka/kafka-logs/
rm -r /tmp/kafka-logs/
rm -r /tmp/zookeeper/

python -c 'import time; time.sleep(10)'

~/samza-hello-samza/bin/grid start zookeeper
~/samza-hello-samza/bin/grid start kafka

KAFKA_PATH="/home/swrrt11/samza-hello-samza/deploy/kafka/bin"

$KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_metrics
$KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_keygroups_status
$KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_metrics --partitions 1 --replication-factor 1
$KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_keygroups_status --partitions 1 --replication-factor 1

python -c 'import time; time.sleep(5)'
