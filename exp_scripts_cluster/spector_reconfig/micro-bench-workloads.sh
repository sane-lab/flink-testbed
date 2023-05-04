#!/bin/bash

source config.sh


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
    mkdir -p ${EXP_DIR}/spector/
    mkdir -p ${EXP_DIR}/raw/
    mkdir -p ${EXP_DIR}/raw/workloads/
    mkdir -p ${EXP_DIR}/results

    #python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/RateAndWindowDelay.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    echo "INFO: dump to ${EXP_DIR}/raw/workloads/${EXP_NAME}"
    if [[ -d ${EXP_DIR}/raw/workloads/${EXP_NAME} ]]; then
        rm -rf ${EXP_DIR}/raw/workloads/${EXP_NAME}
    fi
    mv ${FLINK_DIR}/log ${EXP_DIR}/spector/
    mv ${EXP_DIR}/spector/ ${EXP_DIR}/raw/workloads/${EXP_NAME}
    mkdir -p ${EXP_DIR}/spector/
}

# run one flink demo exp, which is a word count job
run_one_exp() {
  n_tuples=`expr ${runtime} \* ${per_task_rate} \* ${parallelism} \/ ${source_p}`
  # compute n_tuples from per task rates and parallelism
  EXP_NAME=spector-${per_task_rate}-${parallelism}-${max_parallelism}-${per_key_state_size}-${sync_keys}-${replicate_keys_filter}-${state_access_ratio}

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


########### Batch Study ###########

run_batch_key_size_study() {
  # Proactive State replication
  init
  checkpoint_interval=10000000 # by default checkpoint in frequent, trigger only when necessary
  for max_parallelism in 128 256 512 1024 2048; do # 256 512 1024
    affected_keys=`expr ${max_parallelism} \/ ${parallelism} \/ 1` # recompute affected_keys to migrate
    # affected_keys=`expr ${max_parallelism} \/ 2`
    for sync_keys in 1 8 ${affected_keys} ; do # `expr ${affected_keys} \/ 8` ${affected_keys}
      run_one_exp
    done
  done
}

run_batch_state_size() {
  init
  checkpoint_interval=10000000 # by default checkpoint in frequent, trigger only when necessary
  for per_key_state_size in 1024 2048 4096 8196 16384 32768; do
    for sync_keys in 1 8 ${affected_keys}; do
      run_one_exp
    done
  done
}

run_batch_input_rate() {
  init
  checkpoint_interval=10000000 # by default checkpoint in frequent, trigger only when necessary
  for per_task_rate in 1000 2000 4000 8000; do
    for sync_keys in 1 8 ${affected_keys}; do
      run_one_exp
    done
  done
}

########### Replication Study ###########

run_update_state_access_ratio() {
  init
  per_key_state_size=16384 # use a smaller state size to test the insights
  for state_access_ratio in 1 2 4 8 16; do
    for replicate_keys_filter in 1 2 4 8; do
      run_one_exp
    done
  done
}


run_update_state_size() {
  init
#  checkpoint_interval=10000000 # by default checkpoint in frequent, trigger only when necessary
  for per_key_state_size in 1024 2048 4096 8196 16384 32768; do
    for replicate_keys_filter in 1 2 4 8; do
      run_one_exp
    done
  done
}

########### Order Study ###########

run_batch_key_size_study
run_batch_state_size
# run_update_state_access_ratio
# run_update_state_size

# dump the statistics when all exp are finished
# in the future, we will draw the intuitive figures
#python ./analysis/performance_analyzer.py