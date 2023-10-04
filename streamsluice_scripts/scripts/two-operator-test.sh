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
  EXP_NAME=streamsluice-twoOP-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${PERIOD_I}-${N1}-${N2}-${AVERAGE_LENGTH}-${ZIPF_SKEW}-${L}-${migration_overhead}-${epoch}-${is_treat}-${repeat}

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
  migration_interval=2000
  epoch=100
  FLINK_CONF="flink-conf-so1-ss.yaml"
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.MicroBenchmark"
  # only used in script
  runtime=180
  # set in Flink app
  RATE1=60
  TIME1=30
  RATE2=60
  TIME2=30
  RATE_I=80
  TIME_I=120
  PERIOD_I=30
  AVERAGE_LENGTH=10
  ZIPF_SKEW=0.25
  N1=2
  MP1=128
  N2=10
  MP2=128
  repeat=1
  warmup=10000
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${N1} -mp1 ${MP1} -p2 ${N2} -mp2 ${MP2} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} -sentence_length ${AVERAGE_LENGTH} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${N1} -mp1 ${MP1} -p2 ${N2} -mp2 ${MP2} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} -sentence_length ${AVERAGE_LENGTH} &
}

run_scale_test(){
    echo "Run scale test..."
    init
    job="flinkapp.StreamSluiceTestSet.TwoOperatorTest"
    for is_treat in true; do
        for repeat in 1; do
            run_one_exp
        done
    done
}

run_scale_test

