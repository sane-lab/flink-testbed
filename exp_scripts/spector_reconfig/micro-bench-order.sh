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
  EXP_NAME=spector-${per_task_rate}-${per_key_state_size}-${sync_keys}-${replicate_keys_filter}-${order_function}

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

# # initialization of the parameters
# init() {
#   # exp scenario
#   reconfig_scenario="load_balance"

#   # app level
#   JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
#   job="flinkapp.StatefulDemoLongRunKeyRateControlled"
#   runtime=100
#   source_p=1
#   per_task_rate=5000
#   parallelism=2
#   max_parallelism=16
#   key_set=16384
#   per_key_state_size=32768 # byte
#   checkpoint_interval=1000 # by default checkpoint in frequent, trigger only when necessary
#   state_access_ratio=2
#   order_function="default"
#   zipf_skew=1.5

#   # system level
#   operator="Splitter FlatMap"
#   reconfig_start=50000
#   reconfig_interval=10000000
# #  frequency=1 # deprecated
#   affected_tasks=2
#   affected_keys=`expr ${max_parallelism} \/ 2` # `expr ${max_parallelism} \/ 4`
#   sync_keys=0 # disable fluid state migration
#   replicate_keys_filter=0 # replicate those key%filter = 0, 1 means replicate all keys
#   repeat=1
# }


run_order_study() {
  # Fluid Migration with prioritized rules
  init
  job="flinkapp.MicroBenchmarkOrder"
#  per_task_rate=6000
  per_task_rate=5000
  replicate_keys_filter=0
  checkpoint_interval=10000000
  sync_keys=1
  for order_function in default reverse random; do # default reverse
    run_one_exp
  done
}

run_order_zipf_study() {
  # Fluid Migration with prioritized rules
  init
  job="flinkapp.MicroBenchmarkOrder"
  reconfig_scenario="load_balance_zipf"
#  per_task_rate=6000
  per_task_rate=5000
  parallelism=8
  max_parallelism=1024
  replicate_keys_filter=0
  checkpoint_interval=10000000
  sync_keys=32
  zipf_skew=0.6
  for order_function in default reverse; do # default reverse
    run_one_exp
  done
}


#run_order_study
run_order_zipf_study

# dump the statistics when all exp are finished
# in the future, we will draw the intuitive figures
#python ./analysis/performance_analyzer.py