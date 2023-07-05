iscompile=$2

if [ $iscompile == 1 ]
then
    mvn clean package
fi

KAFKA_PATH="/home/samza/samza-hello-samza/deploy/kafka/bin"


$KAFKA_PATH/kafka-topics.sh --delete --zookeeper camel:2181 --topic flink_metrics
$KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_keygroups_status
$KAFKA_PATH/kafka-topics.sh --create --zookeeper camel:2181 -flink_metricsk_metrics --partitions 1 --replication-factor 1

#/home/samza/workspace/flink-extended/build-target/bin/flink run target/testbed-1.0-SNAPSHOT.jar -p2 $1 &
/home/samza/workspace/flink-extended-ete/build-target/bin/flink run target/testbed-1.0-SNAPSHOT.jar -p2 $1 &

python -c 'import time; time.sleep(20)'

./generate.sh dragon:9092 #camel:9092
