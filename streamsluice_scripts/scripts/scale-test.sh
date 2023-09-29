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

# dump data
function analyze() {
    mkdir -p ${EXP_DIR}/raw/
    mkdir -p ${EXP_DIR}/results/

    echo "INFO: dump to ${EXP_DIR}/raw/${EXP_NAME}"
    if [[ -d ${EXP_DIR}/raw/${EXP_NAME} ]]; then
        rm -rf ${EXP_DIR}/raw/${EXP_NAME}
    fi
    mv ${FLINK_DIR}/log ${EXP_DIR}/spector/
    mv ${EXP_DIR}/spector/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/spector/
}

run_one_exp() {
  EXP_NAME=streamsluice-scaletest-${RATE1}-${RATE2}-${RATE_I}-${N1}-${L}-${migration_overhead}-${INTERVAL}-${repeat}

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
  is_treat=1
  is_scalein=1
  vertex_id="22359d48bcb33236cf1e31888091e54c"
  L=2000
  migration_overhead=1000
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.MicroBenchmark"
  # only used in script
  runtime=300
  # set in Flink app
  RATE1=400
  TIME1=60
  RATE2=600
  TIME2=60
  RATE_I=500
  TIME_I=120
  PERIOD_I=240
  N1=5
  MP1=64
  INTERVAL=100
  TOTAL=-1

  repeat=1
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${N1} -mp1 ${MP1} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -srcInterval ${INTERVAL} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${N1} -mp1 ${MP1} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -srcInterval ${INTERVAL} &
}

run_scale-out_test(){
    echo "Run scale-out test..."
    init
    job="flinkapp.StreamSluiceTestSet.ScaleOutTest"
    for repeat in 1; do
        for RATE1 in 400; do
            for CYCLE in 120; do
                run_one_exp
            done
        done
    done
}

