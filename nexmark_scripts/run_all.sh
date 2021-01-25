#!/bin/bash

FLINK_DIR="/home/myc/workspace/flink-related/flink/build-target"
FLINK_APP_DIR="/home/myc/workspace/flink-related/flink-testbed-org"

EXP_DIR="/data"

# run flink clsuter
function runFlink() {
    echo "INFO: starting the cluster"
    if [[ -d ${FLINK_DIR}/log ]]; then
        rm -rf ${FLINK_DIR}/log
    fi
    mkdir ${FLINK_DIR}/log
    ${FLINK_DIR}/bin/start-cluster.sh
}

# clsoe flink clsuter
function stopFlink() {
    echo "INFO: experiment finished, stopping the cluster"
    PID=`jps | grep CliFrontend | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
      kill -9 ${PID}
    fi
    ${FLINK_DIR}/bin/stop-cluster.sh
    mv ${FLINK_DIR}/log ${EXP_DIR}/trisk/
    echo "close finished"
}

# configure parameters in flink bin
function configFlink() {
    # set user requirement
    sed 's/^\(\s*trisk.reconfig.operator.name\s*:\s*\).*/\1'"$operator"'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp1
    sed 's/^\(\s*trisk.reconfig.frequency\s*:\s*\).*/\1'"$frequency"'/' tmp1 > tmp2
    sed 's/^\(\s*trisk.reconfig.affected_tasks\s*:\s*\).*/\1'"$affected_tasks"'/' tmp2 > tmp3
    sed 's/^\(\s*trisk.reconfig.type\s*:\s*\).*/\1'"$reconfig_type"'/' tmp3 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp1 tmp2 tmp3
}

# clean kafka related data
function cleanEnv() {
    rm -rf /tmp/flink*
#    rm ${FLINK_DIR}/log/*
}


# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c flinkapp.StatefulDemoLongRun ${JAR} -runtime ${runtime} -nTuples ${n_tuples} -p2 ${parallelism}"
  ${FLINK_DIR}/bin/flink run -c flinkapp.StatefulDemoLongRun ${JAR} -runtime ${runtime} -nTuples ${n_tuples} -p2 ${parallelism} &
}


# draw figures
function analyze() {
    #python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/RateAndWindowDelay.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    echo "INFO: dump to ${EXP_DIR}/raw/trisk-${reconfig_type}-N${n_tuples}-F${frequency}-T${affected_tasks}"
    if [[ -d ${EXP_DIR}/raw/trisk-${reconfig_type}-N${n_tuples}-F${frequency}-T${affected_tasks} ]]; then
        rm -rf ${EXP_DIR}/raw/trisk-${reconfig_type}-N${n_tuples}-F${frequency}-T${affected_tasks}
    fi
    mv /data/trisk ${EXP_DIR}/raw/trisk-${reconfig_type}-N${n_tuples}-F${frequency}-T${affected_tasks}
    mkdir /data/trisk
}

run_one_exp() {
  echo "INFO: run exp ${reconfig_type} ${frequency} ${runtime} ${n_tuples} ${affected_tasks}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

  analyze
  stopFlink

  python -c 'import time; time.sleep(5)'
}

run_all() {
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  n_tuples=10000000
  runtime=100
  parallelism=10

  # system level
  operator="Splitter FlatMap"
  frequency=5
  affected_tasks=2
  reconfig_type="noop"


  for frequency in 1 2 4 8; do # 0 1 5 10 100
    for n_tuples in 10000000 15000000 20000000; do # 1000000 10000000 100000000
      for reconfig_type in "rescale"; do # "noop" "remap" "rescale"
        for affectedw_tasks in 2; do # 2 4 6 8 10
          run_one_exp
        done
      done
    done
  done
}


run_all

# dump the statistics when all exp are finished
# in the future, we will draw the intuitive figures
python ./analysis/performance_analyzer.py