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
  EXP_NAME=streamsluice-dag-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${PERIOD_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE2}-${L}-${migration_overhead}-${epoch}-${is_treat}-${repeat}

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
  vertex_id="c21234bcbf1e8eb4c61f1927190efebd,22359d48bcb33236cf1e31888091e54c"
  L=1000
  migration_overhead=500
  migration_interval=200
  epoch=100
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StreamSluiceTestSet.MultiOperatorTest"
  # only used in script
  runtime=120
  # set in Flink app
  RATE1=400
  TIME1=30
  RATE2=600
  TIME2=40
  RATE_I=500
  TIME_I=120
  PERIOD_I=240
  AVERAGE_LENGTH=2
  ZIPF_SKEW=0.25
  NKEYS=1000
  STATE_SIZE3=10000
  STATE_SIZE4=10000
  STATE_SIZE5=10000
  N1=1

  N2=1
  MP2=128
  DELAY2=1000
  IO2=1
  STATE_SIZE2=1

  N3=5
  MP3=128
  DELAY3=10000
  IO3=1
  STATE_SIZE3=10000

  N4=1
  MP4=128
  DELAY4=1000
  IO4=1
  STATE_SIZE4=10000

  N5=2
  MP5=128
  DELAY5=2000
  STATE_SIZE5=10000


  repeat=1
  warmup=10000
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY2} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY2} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} &
}

run_scale_test(){
    echo "Run two operator test..."
    init
    migration_overhead=500
    is_treat=false
    repeat=1
    #run_one_exp
    #for L in 750 1000 1250 1500 2000; do
    L=1000
    for PERIOD_I in 15 30 60 120; do
        for is_treat in true; do
            for repeat in 1; do
                run_one_exp
            done
        done
    done
}

run_scale_test


