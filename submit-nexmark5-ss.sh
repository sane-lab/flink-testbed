iscompile=$9

if [ $iscompile == 1 ]
then
    mvn clean package
fi

echo "Src Base $5 Src Rate $3"

KAFKA_PATH="/home/samza/samza-hello-samza/deploy/kafka/bin"


$KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_metrics
$KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_keygroups_status
$KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_metrics --partitions 1 --replication-factor 1
$KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_keygroups_status --partitions 1 --replication-factor 1
/home/samza/workspace/flink-related/flink-extended-ete/build-target/bin/flink run -c Nexmark.queries.Query5 target/testbed-1.0-SNAPSHOT.jar -p2 $1 -mp2 $2 \
 -srcRate $3 -srcCycle $4 -srcBase $5 -srcWarmUp $6 -p-bid-source $7 -window-size $8 &
