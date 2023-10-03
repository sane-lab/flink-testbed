#!/bin/bash

source config.sh

# dump data
function analyze() {
    mkdir -p ${EXP_DIR}/raw/
    mkdir -p ${EXP_DIR}/results/

    echo "INFO: dump to ${EXP_DIR}/raw/${EXP_NAME}"
    if [[ -d ${EXP_DIR}/raw/${EXP_NAME} ]]; then
        rm -rf ${EXP_DIR}/raw/${EXP_NAME}
    fi
    mv ${FLINK_DIR}/log/* ${EXP_DIR}/streamsluice/
    mv ${EXP_DIR}/streamsluice/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/streamsluice/
}

run_one_exp() {
  EXP_NAME=streamsluice-scaletest-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${N1}-${L}-${migration_overhead}-${epoch}-${is_treat}-${repeat}

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
  controller_type=StreamSluice
  #is_treat=true
  is_scalein=true
  vertex_id="0a448493b4782967b150582570326227"
  L=1000
  migration_overhead=500
  migration_interval=500
  epoch=100
  FLINK_CONF="flink-conf-so1-ss.yaml"
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.MicroBenchmark"
  # only used in script
  runtime=600
  # set in Flink app
  RATE1=400
  TIME1=60
  RATE2=400
  TIME2=60
  RATE_I=550
  TIME_I=480
  PERIOD_I=240
  ZIPF_SKEW=0.25
  N1=5
  MP1=64
  repeat=1
  warmup=10000
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${N1} -mp1 ${MP1} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${N1} -mp1 ${MP1} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} &
}

run_scale_out_test(){
    echo "Run scale-out test..."
    init
    job="flinkapp.StreamSluiceTestSet.ScaleOutTest"
    for repeat in 1; do
        for is_treat in true false; do
            for repeat in 1; do
                run_one_exp
            done
        done
    done
}

run_scale_out_test

