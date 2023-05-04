#!/bin/bash

source config.sh

# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \
    -p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} \
    -interval ${checkpoint_interval} -stateAccessRatio ${state_access_ratio} -zipf_skew ${zipf_skew} &"
  ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \
    -p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} \
    -interval ${checkpoint_interval} -stateAccessRatio ${state_access_ratio}  -zipf_skew ${zipf_skew} &
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
  n_tuples=`expr ${runtime} \* ${per_task_rate} \* ${parallelism} \/ ${source_p}`
  # compute n_tuples from per task rates and parallelism
  EXP_NAME=spector-${per_task_rate}-${parallelism}-${max_parallelism}-${per_key_state_size}-${sync_keys}-${replicate_keys_filter}-${state_access_ratio}-${zipf_skew}-${reconfig_scenario}

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
#  job="flinkapp.StatefulDemoLongRunStateControlled"
  job="flinkapp.MicroBenchmarkOverview"
  runtime=120
  source_p=1
  per_task_rate=5000
  parallelism=8
  max_parallelism=512
  key_set=16384
  per_key_state_size=16384 # byte
  checkpoint_interval=1000 # by default checkpoint in frequent, trigger only when necessary
  state_access_ratio=2
  order_function="reverse"
  zipf_skew=1

  # system level
  operator="Splitter FlatMap"
  reconfig_start=10000
  reconfig_interval=10000000
#  frequency=1 # deprecated
  affected_tasks=2
  affected_keys=`expr ${max_parallelism} \/ 4` # `expr ${max_parallelism} \/ 4`
  sync_keys=0 # disable fluid state migration
  replicate_keys_filter=0 # replicate those key%filter = 0, 1 means replicate all keys
  repeat=1
  changelog_enabled=true
}

run_dynamic() {
  # Fluid Migration with prioritized rules
  init
  reconfig_scenario="dynamic"
  replicate_keys_filter=0
  sync_keys=0
  run_one_exp

  # Static Migrate All-at-once
  init
  reconfig_scenario="static"
  replicate_keys_filter=0
  sync_keys=0
  run_one_exp

  # Static Replication
  init
  reconfig_scenario="static"
  replicate_keys_filter=1
  sync_keys=0
  run_one_exp

  # Static Fluid
  init
  reconfig_scenario="static"
  replicate_keys_filter=0
  sync_keys=8
  run_one_exp
}

run_dynamic

# dump the statistics when all exp are finished
# in the future, we will draw the intuitive figures
#python ./analysis/performance_analyzer.py