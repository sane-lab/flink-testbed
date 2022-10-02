#!/bin/bash

FLINK_DIR="/home/myc/workspace/flink-related/spector/build-target"
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

# clean app specific related data
function cleanEnv() {
    rm -rf /tmp/flink*
    rm ${FLINK_DIR}/log/*
}


# clsoe flink clsuter
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
    sed 's/^\(\s*spector.reconfig.affected_keys\s*:\s*\).*/\1'"$affected_keys"'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp1
    sed 's/^\(\s*spector.reconfig.start\s*:\s*\).*/\1'"$reconfig_start"'/' tmp1 > tmp2
    sed 's/^\(\s*spector.reconfig.sync_keys\s*:\s*\).*/\1'"$sync_keys"'/' tmp2 > tmp3
    sed 's/^\(\s*spector.replicate_keys_filter\s*:\s*\).*/\1'"$replicate_keys_filter"'/' tmp3 > tmp4
    sed 's/^\(\s*spector.reconfig.affected_tasks\s*:\s*\).*/\1'"$affected_tasks"'/' tmp4 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp1 tmp2 tmp3 tmp4
}

# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} -interval ${checkpoint_interval} &"
  ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} -interval ${checkpoint_interval} &
}


# draw figures
function analyze() {
    #python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/RateAndWindowDelay.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    echo "INFO: dump to ${EXP_DIR}/raw/${EXP_NAME}"
    if [[ -d ${EXP_DIR}/raw/${EXP_NAME} ]]; then
        rm -rf ${EXP_DIR}/raw/${EXP_NAME}
    fi
    mv ${FLINK_DIR}/log ${EXP_DIR}/spector/
    mv ${EXP_DIR}/spector/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/spector/
}

# run one flink demo exp, which is a word count job
run_one_exp() {
  # compute n_tuples from per task rates and parallelism
  EXP_NAME=spector-${per_key_state_size}-${sync_keys}-${replicate_keys_filter}

  echo "INFO: run exp ${EXP_NAME}"
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

# initialization of the parameters
init() {
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StatefulDemoLongRun"
  runtime=30
  source_p=1
  per_task_rate=9000
  parallelism=2
  max_parallelism=512
  key_set=131072
  per_key_state_size=4096 # byte
  checkpoint_interval=100000 # by default checkpoint in frequent, trigger only when necessary

  n_tuples=`expr ${runtime} \* ${per_task_rate} \* ${parallelism} \/ ${source_p}`


  # system level
  operator="Splitter FlatMap"
  reconfig_start=10000
  reconfig_interval=1000
#  frequency=1 # deprecated
  affected_tasks=2
  affected_keys=`expr ${max_parallelism} \/ 4` # `expr ${max_parallelism} \/ 4`
  sync_keys=0 # disable fluid state migration
  replicate_keys_filter=0 # replicate those key%filter = 0, 1 means replicate all keys
  repeat=1
}

# run the micro benchmarks
run_micro() {
#  # State size
#  init
#  for repeat in 1; do # 1 2 3 4 5
#    for per_key_state_size in 1024 4096 8192 16384; do # state size
#       run_one_exp
#     done
#  done

  # Fluid State Migration Batching keys
  init
  for repeat in 1; do # 1 2 3 4 5
    for sync_keys in 1 4 8 16 32; do # state size 1 4 8 16 32
       run_one_exp
     done
  done

#  # State Replication Evaluation
#  init
#  for repeat in 1; do # 1 2 3 4 5
#    for replicate_keys_filter in 1 2 4 8 0; do # state size 1 2 4 8 0
#       run_one_exp
#     done
#  done

  # Fluid State Migration Batching keys
#  init
#  for repeat in 1; do # 1 2 3 4 5
#    for per_task_rate in 10000 12000 14000 16000; do # state size 1 4 8 16 32
#       run_one_exp
#     done
#  done
}


run_overview() {
  # Migrate at once
  init
  replicate_keys_filter=0
  sync_keys=0
  run_one_exp
  # Fluid Migration
  init
  replicate_keys_filter=0
  sync_keys=8
  run_one_exp
  # Proactive State replication
  init
  replicate_keys_filter=1
  sync_keys=0
  run_one_exp
}


run_micro
#run_overview

# dump the statistics when all exp are finished
# in the future, we will draw the intuitive figures
#python ./analysis/performance_analyzer.py