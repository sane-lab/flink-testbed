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
  EXP_NAME=${how_type}-${runtime}-${CURVE_TYPE}-${RATE1}-${RATE2}-${RATE_I}-${PERIOD_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}-${repeat}

  echo "INFO: run exp ${EXP_NAME}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

  analyze
  stopFlink

  python -c 'import time; time.sleep(1)'
}

# initialization of the parameters
init() {
  # exp scenario
  controller_type=StreamSluice
  whether_type="randomtime"
  how_type="random_3_10_0_1_0"
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
  L=1000
  migration_overhead=500
  migration_interval=5000
  epoch=100
  metrics_output=false
  CURVE_TYPE="sine"
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StreamSluiceTestSet.FourOperatorTest"
  # only used in script
  runtime=50
  # set in Flink app
  RATE1=10000
  TIME1=20
  RATE2=13000
  TIME2=50
  RATE_I=12000
  TIME_I=1
  PERIOD_I=1
  ZIPF_SKEW=0
  NKEYS=1000
  P1=1

  P2=2
  MP2=128
  DELAY2=125
  IO2=1
  STATE_SIZE2=1000

  P3=3
  MP3=128
  DELAY3=250
  IO3=1
  STATE_SIZE3=1000

  P4=6
  MP4=128
  DELAY4=500
  IO4=1
  STATE_SIZE4=1000

  P5=11
  MP5=128
  DELAY5=1000
  STATE_SIZE5=1000


  repeat=1
  warmup=10000
  is_treat=true
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} \
    -curve_type ${CURVE_TYPE} -outputGroundTruth ${outputGroundTruth} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} \
    -curve_type ${CURVE_TYPE} -outputGroundTruth ${outputGroundTruth} &
}

run_multiple_times(){
  repeat=1
  while [ ${repeat} -le 50 ]
  do
    run_one_exp
    printf "${EXP_NAME}\n" >> random-scale.txt
    repeat="$(( ${repeat} + 1 ))"
  done
}

run_random_scale(){
    echo "Run random-scale..."
    init
    whether_type="randomtime"
    CURVE_TYPE="linear"
    printf "" > random-scale.txt
    outputGroundTruth=false
    how_type="random_3_10_0_1_0"
    STATE_SIZE2=50000
    for STATE_SIZE2 in 50000; do
      for how_type in "random_3_10_0_1_0" "random_3_20_0_1_0" "random_3_40_0_1_0"; do
        STATE_SIZE3=${STATE_SIZE2}
        STATE_SIZE4=${STATE_SIZE2}
        STATE_SIZE5=${STATE_SIZE2}

        RATE1=5000
        TIME1=20
        RATE2=5000
        TIME2=50
        RATE_I=10000
        TIME_I=30
        PERIOD_I=30
        P5=12
        run_multiple_times

        RATE1=10000
        TIME1=20
        RATE2=12000
        TIME2=50
        RATE_I=12000
        TIME_I=0
        PERIOD_I=1
        run_multiple_times

        RATE1=10000
        RATE2=13000
        run_multiple_times

        RATE1=10000
        RATE2=14000
        run_multiple_times

        RATE1=10000
        RATE2=14500
        run_multiple_times

        RATE1=10000
        RATE2=15000
        run_multiple_times
      done
    done
}

run_random_scale

