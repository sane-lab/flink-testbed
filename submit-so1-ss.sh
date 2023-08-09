echo $1 $2 ${11}
iscompile=${11}
if [ $iscompile == 1 ]
then
    mvn clean package
fi

echo "Rate1 ${4} Rate2 ${6} RateI ${8}"

KAFKA_PATH="/home/samza/samza-hello-samza/deploy/kafka/bin"


$KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_metrics
$KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_keygroups_status
$KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_metrics --partitions 1 --replication-factor 1
$KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_keygroups_status --partitions 1 --replication-factor 1
#/home/samza/workspace/flink-related/flink-extended-ete/build-target/bin/flink run -c flinkapp.MultiStageLatency target/testbed-1.0-SNAPSHOT.jar -p1 $1 -mp1 $2 -p2 $3 -mp2 $4 -runTime $5 \
# -srcRate $6 -srcPeriod $7 -srcAmplitude $8 -srcWarmUp $9 -srcWarmupRate ${10} -srcInterval ${11} &
/home/samza/workspace/flink-related/flink-extended-ete/build-target/bin/flink run -c flinkapp.StreamSluiceTestSet.ScaleOutTest target/testbed-1.0-SNAPSHOT.jar -p1 $1 -mp1 $2 -phase1Time $3 -phase1Rime $4 -phase2Time $5 \
    -phase2Rate $6 -interTime $7 -interRate $8 -interPeriod $9 -srcInterval ${10} &
