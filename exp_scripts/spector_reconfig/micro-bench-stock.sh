#!/bin/bash

source config.sh

# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -perKeySize ${per_key_state_size} -interval ${checkpoint_interval} &"
  ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -perKeySize ${per_key_state_size} -interval ${checkpoint_interval} &
}


# draw figures
function analyze() {
    mkdir -p ${EXP_DIR}/raw/
    mkdir -p ${EXP_DIR}/results/

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
  EXP_NAME=spector-stock-${parallelism}-${max_parallelism}-${per_key_state_size}-${sync_keys}-${replicate_keys_filter}-${reconfig_scenario}

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
  reconfig_scenario="shuffle" # load_balance

  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="stockv2.StockExchangeApp"
  runtime=120
  source_p=1
  parallelism=16
  max_parallelism=32768
  per_key_state_size=2048 # byte
  checkpoint_interval=1000 # by default checkpoint in frequent, trigger only when necessary

  # system level
  order_function="default"
  zipf_skew=0
  operator="Splitter FlatMap"
  reconfig_start=20000
  reconfig_interval=10000000
#  frequency=1 # deprecated
  affected_tasks=4
  affected_keys=`expr ${max_parallelism} \/ 2` # `expr ${max_parallelism} \/ 4`
  sync_keys=0 # disable fluid state migration
  replicate_keys_filter=0 # replicate those key%filter = 0, 1 means replicate all keys
  repeat=1
  changelog_enabled=true
  window_size=1000000000
  state_backend_async=false
}

run_stock() {
  # Fluid Migration with prioritized rules
  init
  reconfig_scenario="stock"
  replicate_keys_filter=2
  sync_keys=512
  run_one_exp

  # Static Migrate All-at-once
  init
  reconfig_scenario="stock"
  replicate_keys_filter=0
  sync_keys=0
  run_one_exp

  # Static Replication
  init
  reconfig_scenario="stock"
  replicate_keys_filter=1
  sync_keys=0
  run_one_exp

  # Static Fluid
  init
  reconfig_scenario="stock"
  replicate_keys_filter=0
  sync_keys=128
  run_one_exp
}

run_stock

