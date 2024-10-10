#!/usr/bin/env bash
FLINK_DIR="/home/samza/workspace/flink-related/flink-extended-ete/build-target"
FLINK_APP_DIR="/home/samza/workspace/flink-related/flink-testbed-sane"
SCRIPT_DIR="/home/samza/workspace/flink-related/flink-testbed-sane/streamsluice_scripts"
FLINK_CONF_DIR="${SCRIPT_DIR}/conf-server"

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
    sed 's/^\(\s*controller.how.type\s*:\s*\).*/\1'"$how_type"'/' tmp10 > tmp11
    sed 's/^\(\s*streamsluice.metrics.is_output\s*:\s*\).*/\1'"$metrics_output"'/' tmp11 > tmp12
    sed 's/^\(\s*streamsluice.system.spike_estimation\s*:\s*\).*/\1'"$spike_estimation"'/' tmp12 > tmp13
    sed 's/^\(\s*streamsluice.system.spike_estimation_slope\s*:\s*\).*/\1'"$spike_slope"'/' tmp13 > tmp14
    sed 's/^\(\s*streamsluice.system.spike_estimation_intercept\s*:\s*\).*/\1'"$spike_intercept"'/' tmp14 > tmp15
    sed 's/^\(\s*streamsluice.system.autotune\s*:\s*\).*/\1'"$autotune"'/' tmp15 > tmp16
    sed 's/^\(\s*streamsluice.system.autotune.interval\s*:\s*\).*/\1'"$autotune_interval"'/' tmp16 > tmp17
    sed 's/^\(\s*model.max_parallelism.a84740bacf923e828852cc4966f2247c\s*:\s*\).*/\1'"$LP2"'/' tmp17 > tmp18
    sed 's/^\(\s*model.max_parallelism.eabd4c11f6c6fbdf011f0f1fc42097b1\s*:\s*\).*/\1'"$LP3"'/' tmp18 > tmp19
    sed 's/^\(\s*model.max_parallelism.d01047f852abd5702a0dabeedac99ff5\s*:\s*\).*/\1'"$LP4"'/' tmp19 > tmp20
    sed 's/^\(\s*model.max_parallelism.d2336f79a0d60b5a4b16c8769ec82e47\s*:\s*\).*/\1'"$LP5"'/' tmp20 > tmp21
    sed 's/^\(\s*streamsluice.model.decision_interval\s*:\s*\).*/\1'"$decision_interval"'/' tmp21 > tmp22
    sed 's/^\(\s*streamsluice.metrics.snapshot_size\s*:\s*\).*/\1'"$snapshot_size"'/' tmp22 > tmp23
    sed 's/^\(\s*controller.how.more_optimizaiton\s*:\s*\).*/\1'"$how_more_optimization_flag"'/' tmp23 > tmp24
    sed 's/^\(\s*controller.how.optimizaiton\s*:\s*\).*/\1'"$how_optimization_flag"'/' tmp24 > tmp25
    sed 's/^\(\s*controller.how.conservative\s*:\s*\).*/\1'"$how_conservative_flag"'/' tmp25 > tmp26
    sed 's/^\(\s*controller.steady_limit\s*:\s*\).*/\1'"$how_intrinsic_bound_flag"'/' tmp26 > tmp27
    sed 's/^\(\s*model.coordination_latency_flag\s*:\s*\).*/\1'"$coordination_latency_flag"'/' tmp27 > tmp28
    sed 's/^\(\s*model.conservative_service_rate_flag\s*:\s*\).*/\1'"$conservative_service_rate_flag"'/' tmp28 > tmp29
    sed 's/^\(\s*streamsluice.system.backlog_smooth\s*:\s*\).*/\1'"$smooth_backlog_flag"'/' tmp29 > tmp30
    sed 's/^\(\s*streamsluice.metrics.use_new_retriever\s*:\s*\).*/\1'"$new_metrics_retriever_flag"'/' tmp30 > tmp31
    sed 's/^\(\s*streamsluice.system.autotuner\s*:\s*\).*/\1'"$autotuner"'/' tmp31 > tmp32
    sed 's/^\(\s*streamsluice.system.autotune.latency_window\s*:\s*\).*/\1'"$autotuner_latency_window"'/' tmp32 > tmp33
    sed 's/^\(\s*streamsluice.system.autotune.bar_lowerbound\s*:\s*\).*/\1'"$autotuner_bar_lowerbound"'/' tmp33 > tmp34
    sed 's/^\(\s*streamsluice.system.autotune.initial_value_option\s*:\s*\).*/\1'"$autotuner_initial_value_option"'/' tmp34 > tmp35
    sed 's/^\(\s*streamsluice.system.autotune.adjustment_option\s*:\s*\).*/\1'"$autotuner_adjustment_option"'/' tmp35 > tmp36
    sed 's/^\(\s*streamsluice.system.autotune.increase_bar_option\s*:\s*\).*/\1'"$autotuner_increase_bar_option"'/' tmp36 > tmp37
    sed 's/^\(\s*streamsluice.system.autotune.initial_value_alpha\s*:\s*\).*/\1'"$autotuner_initial_value_alpha"'/' tmp37 > tmp38
    sed 's/^\(\s*streamsluice.system.autotune.adjustment_beta\s*:\s*\).*/\1'"$autotuner_adjustment_beta"'/' tmp38 > tmp39
    sed 's/^\(\s*streamsluice.system.autotune.increase_bar_alpha\s*:\s*\).*/\1'"$autotuner_increase_bar_alpha"'/' tmp39 > tmp40
    sed 's/^\(\s*controller.scale_in.type\s*:\s*\).*/\1'"$scalein_type"'/' tmp40 > ${FLINK_CONF_DIR}/flink-conf.yaml

    rm tmp*
    echo ${FLINK_CONF_DIR}/flink-conf.yaml
    cp ${FLINK_CONF_DIR}/* ${FLINK_DIR}/conf
    for host in "dragon" "eagle"; do
      scp ${FLINK_CONF_DIR}/* ${host}:${FLINK_DIR}/conf
      scp ${FLINK_CONF_DIR}/flink-conf-slave.yaml ${host}:${FLINK_DIR}/conf/flink-conf.yaml
    done
}

# clean kafka related data
function cleanEnv() {
    KAFKA_PATH="${HELLOSAMZA_DIR}/deploy/kafka/bin";
    for host in "camel"; do
      script="
        rm -rf /tmp/flink*;
        rm ${FLINK_DIR}/log/*;
        export JAVA_HOME=/home/samza/kit/jdk;
        ${HELLOSAMZA_DIR}/bin/grid stop kafka;
        ${HELLOSAMZA_DIR}/bin/grid stop zookeeper;
        kill -9 $(jps |grep Kafka|awk '{print $1}');
        rm -rf /data/kafka/kafka-logs/;
        rm -r /tmp/kafka-logs/;
        rm -r /tmp/zookeeper/;

        python -c 'import time; time.sleep(2)';

        ${HELLOSAMZA_DIR}/bin/grid start zookeeper;
        ${HELLOSAMZA_DIR}/bin/grid start kafka;



        ${KAFKA_PATH}/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_metrics;
        ${KAFKA_PATH}/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_keygroups_status;
        ${KAFKA_PATH}/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_metrics --partitions 1 --replication-factor 1;
        ${KAFKA_PATH}/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_keygroups_status --partitions 1 --replication-factor 1;

        python -c 'import time; time.sleep(1)'
      "
      ssh ${host} "${script}"
    done
}



