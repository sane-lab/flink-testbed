#!/bin/bash

source config.sh

# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} \-p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -interval ${checkpoint_interval} &"
  ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} \-p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -interval ${checkpoint_interval} -srcBase ${srcBase} -srcRate 0 &
}

# run applications
function runQ8() {
  srcBase=`expr ${per_task_rate} \* ${parallelism} \/ ${source_p} \/ 2`

  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} \-p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -interval ${checkpoint_interval} -auction-srcBase ${srcBase} -auction-srcRate 0 \
    -person-srcBase ${srcBase} -person-srcRate 0"
  ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} \-p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -interval ${checkpoint_interval} -auction-srcBase ${srcBase} -auction-srcRate 0 \
    -person-srcBase ${srcBase} -person-srcRate 0 &
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
  EXP_NAME=spector-nexmark-query${query_id}-${sync_keys}-${replicate_keys_filter}

  echo "INFO: run exp ${EXP_NAME}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  if [ ${query_id} == 8 ]
  then
     runQ8
  else
     runApp
  fi


  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

  analyze
  stopFlink

  python -c 'import time; time.sleep(5)'
}

# initialization of the parameters
init() {
  # exp scenario
  reconfig_scenario="shuffle"

  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.MicroBenchmark"
  runtime=50
  source_p=1
  parallelism=8
  max_parallelism=32768
  # per_task_rate=1000
  per_task_rate=2500
  checkpoint_interval=10000 # by default checkpoint in frequent, trigger only when necessary

  srcBase=`expr ${per_task_rate} \* ${parallelism} \/ ${source_p}`

  # system level
  operator="Mapper"
  reconfig_start=50000
  reconfig_interval=10000000
#  frequency=1 # deprecated
  affected_tasks=8
  # affected_keys=`expr ${max_parallelism} \/ ${parallelism} \/ 1` # `expr ${max_parallelism} \/ 4`
  affected_keys=`expr ${max_parallelism} \/ 2` # `expr ${max_parallelism} \/ 4`
  sync_keys=0 # disable fluid state migration
  replicate_keys_filter=0 # replicate those key%filter = 0, 1 means replicate all keys
  repeat=1
  changelog_enabled=true
  window_size=1000000000 # for reporting the system overhead instead of the window in operators
  state_backend_async=false
  zipf_skew=1
}

run_query1() {
#  init
  job="Nexmark.queries.Query1"
  operator="Mapper"
  run_one_exp
}

run_query2() {
#  init
  job="Nexmark.queries.Query2"
  operator="Splitter FlatMap"
  run_one_exp
}

run_query5() {
#  init
  job="Nexmark.queries.Query5"
  operator="window"
  run_one_exp
}

run_query8() {
#  init
  job="Nexmark.queries.Query8"
  operator="join"
  run_one_exp
}

run_query11() {
#  init
  job="Nexmark.queries.Query11"
  operator="window"
  run_one_exp
}


nexmark_overview() {
  for query_id in 8; do # 1 2 5 8 11
  
   # Migrate at once
   init
   replicate_keys_filter=0
   sync_keys=0
#    per_task_rate=5000
   checkpoint_interval=10000000
   run_query${query_id}

   # Fluid Migration
   init
   replicate_keys_filter=0
   sync_keys=1
#    per_task_rate=20000
   checkpoint_interval=10000000
   run_query${query_id}

    # Proactive State replication
    init
    replicate_keys_filter=1
    sync_keys=0
#    per_task_rate=20000
    run_query${query_id}

    # Spacker
   init
   replicate_keys_filter=0
   sync_keys=4 # 8 for nexmark Q8
#    per_task_rate=20000
   checkpoint_interval=10000000
   run_query${query_id}

  done
}

nexmark_overview