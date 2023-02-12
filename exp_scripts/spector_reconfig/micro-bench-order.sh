#!/bin/bash

FLINK_DIR="/home/myc/workspace/Spector/build-target"
FLINK_APP_DIR="/home/myc/workspace/flink-testbed"

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
    sed 's/^\(\s*controller.target.operators\s*:\s*\).*/\1'"$operator"'/' tmp4 > tmp5
    sed 's/^\(\s*spector.reconfig.order_function\s*:\s*\).*/\1'"$order_function"'/' tmp5 > tmp6
    sed 's/^\(\s*spector.reconfig.scenario\s*:\s*\).*/\1'"$reconfig_scenario"'/' tmp6 > tmp7
    sed 's/^\(\s*spector.reconfig.affected_tasks\s*:\s*\).*/\1'"$affected_tasks"'/' tmp7 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp1 tmp2 tmp3 tmp4 tmp5 tmp6 tmp7
}

# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} -interval ${checkpoint_interval} -stateAccessRatio ${state_access_ratio} &"
  ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} -interval ${checkpoint_interval} -stateAccessRatio ${state_access_ratio} &
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
  n_tuples=`expr ${runtime} \* ${per_task_rate} \* ${parallelism} \/ ${source_p}`
  # compute n_tuples from per task rates and parallelism
  EXP_NAME=spector-$per_task_rate-${per_key_state_size}-${sync_keys}-${replicate_keys_filter}-${order_function}

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
  # exp scenario
  reconfig_scenario="load_balance"

  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StatefulDemoLongRunrKeyRateControlled"
  runtime=100
  source_p=1
  per_task_rate=5000
  parallelism=2
  max_parallelism=8
  key_set=16384
  per_key_state_size=32768 # byte
  checkpoint_interval=1000 # by default checkpoint in frequent, trigger only when necessary
  state_access_ratio=2
  order_function="default"



  # system level
  operator="Splitter FlatMap"
  reconfig_start=50000
  reconfig_interval=10000000
#  frequency=1 # deprecated
  affected_tasks=2
  affected_keys=`expr ${max_parallelism} \/ 2` # `expr ${max_parallelism} \/ 4`
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

#  # Fluid State Migration Batching keys
#  init
#  for repeat in 1; do # 1 2 3 4 5
#    for sync_keys in 1 4 8 16 32; do # state size 1 4 8 16 32
#       run_one_exp
#     done
#  done

  # State Replication Evaluation
  init
  for repeat in 1; do # 1 2 3 4 5
    for replicate_keys_filter in 1 2 4 8 0; do # state size 1 2 4 8 0
       run_one_exp
     done
  done

  # Fluid State Migration Batching keys
#  init
#  for repeat in 1; do # 1 2 3 4 5
#    for per_task_rate in 10000 12000 14000 16000; do # state size 1 4 8 16 32
#       run_one_exp
#     done
#  done
}

run_test() {
#  init
#  for repeat in 1; do # 1 2 3 4 5
#    for per_key_state_size in 4096 8192 16384 32768; do # state size 1 2 4 8 0
#       run_one_exp
#     done
#  done

  init
  state_access_ratio=100
  checkpoint_interval=10000000
  for repeat in 1; do # 1 2 3 4 5
    for per_task_rate in 7000 8000 9000; do # state size 1 2 4 8 0
       run_one_exp
     done
  done
}

run_no_migration() {
#  init
#  for repeat in 1; do # 1 2 3 4 5
#    for per_key_state_size in 4096 8192 16384 32768; do # state size 1 2 4 8 0
#       run_one_exp
#     done
#  done

  init
#  state_access_ratio=100
  checkpoint_interval=10000000
#  reconfig_start=10000000
  for repeat in 1; do # 1 2 3 4 5
    for per_task_rate in 6000 7000 8000 9000 10000 12000 15000; do # state size 1 2 4 8 0
       run_one_exp
     done
  done
}

run_replication_overhead() {
  # Migrate at once
  init
  replicate_keys_filter=0
  sync_keys=0
  reconfig_start=10000000
  run_one_exp

  # Proactive State replication
  init
  replicate_keys_filter=1
  sync_keys=0
  reconfig_start=10000000
  run_one_exp
}


run_overview() {
  # Migrate at once
  init
  replicate_keys_filter=0
  sync_keys=0
  checkpoint_interval=10000000
  run_one_exp
  # Fluid Migration
  init
  replicate_keys_filter=0
  sync_keys=8
  checkpoint_interval=10000000
  run_one_exp
  # Proactive State replication
  init
  replicate_keys_filter=1
  sync_keys=0
  run_one_exp
}


run_fluid_study() {
  # Fluid Migration
  init
  replicate_keys_filter=0
  checkpoint_interval=10000000
  state_access_ratio=20
  for sync_keys in 4 8 16 32 64 128; do
    run_one_exp
  done
}

run_replication_study() {
  # Proactive State replication
  init
  sync_keys=0
  for replicate_keys_filter in 1 2 4 8; do
    run_one_exp
  done
}


run_order_study() {
  # Fluid Migration with prioritized rules
  init
  per_task_rate=6000
  replicate_keys_filter=0
  checkpoint_interval=10000000
  sync_keys=1
  for order_function in default reverse; do
    run_one_exp
  done
}


#run_micro
#run_overview
#run_test
#run_replication_overhead
#run_fluid_study
#run_replication_study
#run_no_migration
run_order_study

# dump the statistics when all exp are finished
# in the future, we will draw the intuitive figures
#python ./analysis/performance_analyzer.py