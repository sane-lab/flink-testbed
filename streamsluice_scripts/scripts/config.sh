#!/usr/bin/env bash
FLINK_DIR="/home/samza/workspace/flink-related/flink-extended-ete/build-target"
FLINK_APP_DIR="/home/samza/workspace/flink-related/flink-testbed-sane"
SCRIPT_DIR="/home/samza/workspace/flink-related/flink-testbed-sane/streamsluice_scripts"
FLINK_CONF_DIR="${SCRIPT_DIR}/conf-local"

EXP_DIR="/data/streamsluice"

HELLOSAMZA_DIR="/home/samza/samza-hello-samza"

# run flink clsuter
function runFlink() {
    echo "INFO: starting the cluster"
    if [[ -d ${FLINK_DIR}/log ]]; then
        rm -rf ${FLINK_DIR}/log
    fi
    mkdir ${FLINK_DIR}/log
    ${FLINK_DIR}/bin/start-cluster.sh
}

function stopFlink() {
    echo "INFO: experiment finished, stopping the cluster"
    PID=`jps | grep CliFrontend | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
      kill -9 ${PID}
    fi
    ${FLINK_DIR}/bin/stop-cluster.sh
    echo "close finished"
    cleanEnv
}

# configure parameters in flink bin
function configFlink() {
    # set user requirement
    echo $L
    sed 's/^\(\s*streamsluice.requirement.latency\s*:\s*\).*/\1'"$L"'/' ${FLINK_CONF_DIR}/flink-conf.yaml > tmp1
    echo $migration_overhead
    sed 's/^\(\s*streamsluice.system.migration_overhead\s*:\s*\).*/\1'"$migration_overhead"'/' tmp1 > tmp2
    echo $vertex_id
    sed 's/^\(\s*model.vertex\s*:\s*\).*/\1'"$vertex_id"'/' tmp2 > tmp3
    sed 's/^\(\s*streamsluice.system.metrics_interval\s*:\ss*\).*/\1'"$epoch"'/' tmp3 > tmp4
    sed 's/^\(\s*model.warmup\s*:\s*\).*/\1'"$warmup"'/' tmp4 > tmp5
    sed 's/^\(\s*streamsluice.system.migration_interval\s*:\s*\).*/\1'"$migration_interval"'/' tmp5 > tmp6
    sed 's/^\(\s*streamsluice.system.is_treat\s*:\s*\).*/\1'"$is_treat"'/' tmp6 > tmp7
    sed 's/^\(\s*streamsluice.system.is_scalein\s*:\s*\).*/\1'"$is_scalein"'/' tmp7 > tmp8
    sed 's/^\(\s*controller.type\s*:\s*\).*/\1'"$controller_type"'/' tmp8 > tmp9
    sed 's/^\(\s*controller.whether.type\s*:\s*\).*/\1'"$whether_type"'/' tmp9 > tmp10
    sed 's/^\(\s*controller.how.type\s*:\s*\).*/\1'"$how_type"'/' tmp10 > ${FLINK_CONF_DIR}/flink-conf.yaml
    rm tmp*
    echo ${FLINK_CONF_DIR}/flink-conf.yaml
    cp ${FLINK_CONF_DIR}/* ${FLINK_DIR}/conf
}

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

    python -c 'import time; time.sleep(10)'

    $HELLOSAMZA_DIR/bin/grid start zookeeper
    $HELLOSAMZA_DIR/bin/grid start kafka

    KAFKA_PATH="${HELLOSAMZA_DIR}/deploy/kafka/bin"

    $KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_metrics
    $KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_keygroups_status
    $KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_metrics --partitions 1 --replication-factor 1
    $KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_keygroups_status --partitions 1 --replication-factor 1

    python -c 'import time; time.sleep(5)'
}



