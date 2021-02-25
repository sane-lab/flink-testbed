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
    cleanEnv
}

# clean app specific related data
function cleanEnv() {
    rm -rf /tmp/flink*
    rm ${FLINK_DIR}/log/*
}


# configure parameters in flink bin
function configFlink() {
    # set user requirement
    sed 's/^\(\s*trisk.reconfig.operator.name\s*:\s*\).*/\1'"$operator"'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp1
    sed 's/^\(\s*trisk.reconfig.interval\s*:\s*\).*/\1'"$reconfig_interval"'/' tmp1 > tmp2
    sed 's/^\(\s*trisk.reconfig.affected_tasks\s*:\s*\).*/\1'"$affected_tasks"'/' tmp2 > tmp3
    sed 's/^\(\s*trisk.reconfig.type\s*:\s*\).*/\1'"$reconfig_type"'/' tmp3 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp1 tmp2 tmp3
}

# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c flinkapp.StatefulDemoLongRun ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} &"
  ${FLINK_DIR}/bin/flink run -c flinkapp.StatefulDemoLongRun ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} &
}


# draw figures
function analyze() {
    #python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/RateAndWindowDelay.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    echo "INFO: dump to ${EXP_DIR}/raw/trisk-${reconfig_type}-${reconfig_interval}-${runtime}-${parallelism}-${per_task_rate}-${key_set}-${per_key_state_size}-${affected_tasks}"
    if [[ -d ${EXP_DIR}/raw/trisk-${reconfig_type}-${reconfig_interval}-${runtime}-${parallelism}-${per_task_rate}-${key_set}-${per_key_state_size}-${affected_tasks} ]]; then
        rm -rf ${EXP_DIR}/raw/trisk-${reconfig_type}-${reconfig_interval}-${runtime}-${parallelism}-${per_task_rate}-${key_set}-${per_key_state_size}-${affected_tasks}
    fi
    mv ${EXP_DIR}/trisk/ ${EXP_DIR}/raw/trisk-${reconfig_type}-${reconfig_interval}-${runtime}-${parallelism}-${per_task_rate}-${key_set}-${per_key_state_size}-${affected_tasks}
    mkdir ${EXP_DIR}/trisk/
}

run_one_exp() {
  # compute n_tuples from per task rates and parallelism
  n_tuples=`expr ${runtime} \* ${per_task_rate} \* ${parallelism} \/ ${source_p}`

  echo "INFO: run exp ${reconfig_type}-${reconfig_interval}-${runtime}-${parallelism}-${per_task_rate}-${key_set}-${per_key_state_size}-${affected_tasks}"
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

init() {
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  runtime=150
  source_p=5
#  n_tuples=15000000
  per_task_rate=6000
  parallelism=10
  key_set=1000
  per_key_state_size=1024 # byte

  # system level
  operator="Splitter FlatMap"
  reconfig_interval=10000
  reconfig_type="remap"
#  frequency=1 # deprecated
  affected_tasks=2
}

run_micro() {
#  init
#
#  for parallelism in 5 10 20; do
#    run_one_exp
#  done
#
#  init
#
#  for per_task_rate in 1000 2000 4000 6000 8000; do
#    run_one_exp
#  done
#
  init

  for affected_tasks in 2 4 6 8 10; do # 2 4 6 8 10
    run_one_exp
  done

#  init
#
#  for per_key_state_size in 1024 10240 20480 40960; do
#    run_one_exp
#  done
}

run_micro

# dump the statistics when all exp are finished
# in the future, we will draw the intuitive figures
python ./analysis/performance_analyzer.py