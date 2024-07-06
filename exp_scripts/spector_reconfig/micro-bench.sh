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
  for repeat in 1; do # 1 2 3 4 5
    for per_task_rate in 7000 8000 9000; do # state size 1 2 4 8 0
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
  state_access_ratio=2
  checkpoint_interval=5000
  run_one_exp

  # Proactive State replication
  init
  replicate_keys_filter=1
  sync_keys=0
  reconfig_start=10000000
  state_access_ratio=2
  checkpoint_interval=5000
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
  # max_parallelism=16384
  # key_set=1048576
  # per_key_state_size=1024
  replicate_keys_filter=0
  checkpoint_interval=10000000
  state_access_ratio=100
  affected_keys=`expr ${max_parallelism} \/ 2` # `expr ${max_parallelism} \/ 4`
 for sync_keys in 1 2 4 8 16 32 64; do
  # for sync_keys in `expr ${max_parallelism} \/ 8 \/ 1` `expr ${max_parallelism} \/ 8 \/ 2` `expr ${max_parallelism} \/ 8 \/ 4` `expr ${max_parallelism} \/ 8 \/ 8` `expr ${max_parallelism} \/ 8 \/ 16` `expr ${max_parallelism} \/ 8 \/ 32` `expr ${max_parallelism} \/ 8 \/ 64`; do #  2 4 8 16 32 64 128
  # for sync_keys in `expr ${max_parallelism} \/ 8 \/ 64`; do #  2 4 8 16 32 64 128
    run_one_exp
  done
}

run_replication_study() {
  # Proactive State replication
  init
  # max_parallelism=16384
  # key_set=1048576
  # per_key_state_size=1024
  # replicate_keys_filter=0
  # checkpoint_interval=1000
  # sync_keys=0
  checkpoint_interval=1000
  state_access_ratio=2
  key_set=32768
  affected_keys=`expr ${max_parallelism}` # `expr ${max_parallelism} \/ 4`
  for replicate_keys_filter in 0 1 2 4 8; do # 1 2 4 8
#  for replicate_keys_filter in 1; do
    run_one_exp
  done
}

run_state_size() {
  init
  replicate_keys_filter=0
  sync_keys=0
  for per_key_state_size in 1024 2048 4096 8196 16384 32768; do
    run_one_exp
  done
}

run_rate() {
  init
  replicate_keys_filter=0
  sync_keys=0
  for per_task_rate in 5000 6000 7000 8000 9000 10000; do
    run_one_exp
  done
}

run_access_ratio() {
  init
  replicate_keys_filter=0
  sync_keys=0
  for state_access_ratio in 2 10 25 50 100; do
    run_one_exp
  done
}

#run_parallelism() {
#  init
#  replicate_keys_filter=0
#  sync_keys=0
#  for parallelism in 2 4 8 16; do
#    affected_tasks=$parallelism
#    run_one_exp
#  done
#}


#run_micro
# run_overview
#run_test
#run_replication_overhead
run_fluid_study
run_replication_study

# dump the statistics when all exp are finished
# in the future, we will draw the intuitive figures
#python ./analysis/performance_analyzer.py