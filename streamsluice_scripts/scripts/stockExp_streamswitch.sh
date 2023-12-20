#!/bin/bash

FLINK_DIR="/home/samza/workspace/flink-related/flink-extended-streamswitch/build-target"
FLINK_APP_DIR="/home/samza/workspace/flink-related/flink-testbed-sane"

# clean kafka related data
function cleanEnv() {
    rm -rf /tmp/flink*
    rm ${FLINK_DIR}/log/*
    export JAVA_HOME=/home/samza/kit/jdk
    $HELLOSAMZA_DIR/bin/grid stop kafka
    $HELLOSAMZA_DIR/bin/grid stop zookeeper
    kill -9 $(jps |grep Kafka|awk '{print $1}')
    rm -rf /data/kafka/kafka-logs/
    rm -r /tmp/kafka-logs/
    rm -r /tmp/zookeeper/

    python -c 'import time; time.sleep(2)'

    $HELLOSAMZA_DIR/bin/grid start zookeeper
    $HELLOSAMZA_DIR/bin/grid start kafka

    KAFKA_PATH="${HELLOSAMZA_DIR}/deploy/kafka/bin"

    $KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_metrics
    $KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_keygroups_status
    $KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_metrics --partitions 1 --replication-factor 1
    $KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_keygroups_status --partitions 1 --replication-factor 1

    python -c 'import time; time.sleep(1)'
}

# configure parameters in flink bin
function configFlink() {
    # set user requirement
    sed 's/^\(\s*model.vertex\s*:\s*\).*/\1'"a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5"'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
    sed 's/^\(\s*streamswitch.requirement.latency.a84740bacf923e828852cc4966f2247c\s*:\s*\).*/\1'"$L1"'/' tmp > tmp1
    sed 's/^\(\s*streamswitch.requirement.latency.eabd4c11f6c6fbdf011f0f1fc42097b1\s*:\s*\).*/\1'"$L2"'/' tmp1 > tmp2
    sed 's/^\(\s*streamswitch.requirement.latency.d01047f852abd5702a0dabeedac99ff5\s*:\s*\).*/\1'"$L3"'/' tmp2 > tmp3
    sed 's/^\(\s*streamswitch.system.l_low\s*:\s*\).*/\1'"$l_low"'/' tmp3 > tmp4
    sed 's/^\(\s*streamswitch.system.l_high\s*:\s*\).*/\1'"$l_high"'/' tmp4 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp tmp*

    # set static or streamswitch
    if [[ ${isTreat} == 1 ]]
    then
        sed 's/^\(\s*streamswitch.system.is_treat\s*:\s*\).*/\1'true'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
        mv tmp ${FLINK_DIR}/conf/flink-conf.yaml
    elif [[ ${isTreat} == 0 ]]
    then
        sed 's/^\(\s*streamswitch.system.is_treat\s*:\s*\).*/\1'false'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
        mv tmp ${FLINK_DIR}/conf/flink-conf.yaml
    fi
}

# run flink clsuter
function runFlink() {
    if [[ -d ${FLINK_DIR}/log ]]; then
        rm -rf ${FLINK_DIR}/log
    fi
    mkdir ${FLINK_DIR}/log
    ${FLINK_DIR}/bin/start-cluster.sh
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4KeyStateSize ${STATE_SIZE4} \
    -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4KeyStateSize ${STATE_SIZE4} \
    -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} &
}

# clsoe flink clsuter
function closeFlink() {
    echo "INFO: experiment finished, stopping the cluster"
    PID=`jps | grep CliFrontend | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
      kill -9 ${PID}
    fi
    ${FLINK_DIR}/bin/stop-cluster.sh
    echo "close finished"
}


function analyze() {
    EXP_DIR="/data/streamsluice"
    mkdir -p ${EXP_DIR}/raw/
    mkdir -p ${EXP_DIR}/results/

    echo "INFO: dump to ${EXP_DIR}/raw/${EXP_NAME}"
    if [[ -d ${EXP_DIR}/raw/${EXP_NAME} ]]; then
        rm -rf ${EXP_DIR}/raw/${EXP_NAME}
    fi
    mv ${FLINK_DIR}/log/* ${EXP_DIR}/streamsluice/
    mv ${EXP_DIR}/streamsluice/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/streamsluice/
}
# set in Flink
L=2000
L1=666
L2=666
L3=666
l_low=100
l_high=100
isTreat=1
# app level
JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
job="flinkapp.StreamSluiceTestSet.StockTest"
# only used in script
runtime=3690
warmup_rate=2000
warmup_time=30
skip_interval=20
# set in Flink app
stock_path="/home/samza/SSE_data/"
stock_file_name="sb-4hr-50ms.txt"
P1=1

P2=3
MP2=128
DELAY2=1000
IO2=1
STATE_SIZE2=100

P3=5
MP3=128
DELAY3=2000
IO3=1
STATE_SIZE3=100

P4=12
MP4=128
DELAY4=5000
IO4=1
STATE_SIZE4=100

repeat=1
warmup=10000


# only used in script


# set in Flink app
EXP_NAME=stock-${stock_file_name}-streamswitch-${runtime}-${warmup_time}-${warmup_rate}-${skip_interval}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${L}-${epoch}-${is_treat}-${repeat}
echo ${EXP_NAME}

cleanEnv
configFlink
runFlink
runApp

SCRIPTS_RUNTIME=`expr ${runtime} + 10`
python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

analyze
closeFlink

~/samza-hello-samza/deploy/kafka/bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic flink_metrics --from-beginning > ./nexmark_scripts/draw/logs/${EXP_NAME}/metrics &
python -c 'import time; time.sleep(30)'
kill -9 $(jps | grep ConsoleConsumer | awk '{print $1}')
