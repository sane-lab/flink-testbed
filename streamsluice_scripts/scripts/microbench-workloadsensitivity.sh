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
  EXP_NAME=microbench-workload-${GRAPH}-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${RANGE_I}-${PERIOD_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${L}-${migration_interval}-${epoch}-${is_treat}-${repeat}

  echo "INFO: run exp ${EXP_NAME}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

  analyze
  echo "${EXP_NAME}" > completedExps
  stopFlink

  python -c 'import time; time.sleep(5)'
}

# initialization of the parameters
init() {
  # exp scenario
  controller_type=StreamSluice
  whether_type="streamsluice"
  how_type="streamsluice"
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
  L=1000
  migration_interval=500
  epoch=100
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StreamSluiceTestSet.MicroBench"
  # only used in script
  runtime=300
  # set in Flink app
  GRAPH=2op
  RATE1=10000
  TIME1=30
  RATE2=10000
  TIME2=40
  RATE_I=10000
  RANGE_I=2000
  TIME_I=240
  PERIOD_I=120
  ZIPF_SKEW=0
  NKEYS=1000
  P1=1

  P2=1
  MP2=128
  DELAY2=50 #266
  IO2=1
  STATE_SIZE2=100

  P3=1
  MP3=128
  DELAY3=50
  IO3=1
  STATE_SIZE3=100

  P4=12
  MP4=128
  DELAY4=1000
  IO4=1
  STATE_SIZE4=100

  P5=1
  MP5=128
  DELAY5=50
  IO5=1
  STATE_SIZE5=100

  spike_estimation="linear_regression"
  spike_slope=0.7
  spike_intercept=200
  is_treat=true
  repeat=1
  warmup=10000
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -graph ${GRAPH} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interRange ${RANGE_I} -interPeriod ${PERIOD_I} -inter_delta ${DELTA_I} \
    -zipf_skew ${ZIPF_SKEW} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -graph ${GRAPH} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interRange ${RANGE_I} -interPeriod ${PERIOD_I} -inter_delta ${DELTA_I} \
    -zipf_skew ${ZIPF_SKEW} &
}

run_scale_test(){
    echo "Run micro bench workload sensitivity..."
    init
    # Range difference autotune
    runtime=3660
    L=1000
    TIME1=30
    TIME2=40
    TIME_I=3600
    GRAPH=1split2
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5"
    LP2=4
    LP3=4
    LP4=51
    P2=1
    P3=1
    P4=12
    DELTA_I=0
    PERIOD_I=120
    RATE1=10000
    RATE2=10000
    RATE_I=10000
#    STATE_SIZE2=100
#    STATE_SIZE3=100
#    STATE_SIZE4=100
    STATE_SIZE2=10000
    STATE_SIZE3=10000
    STATE_SIZE4=10000
    STATE_SIZE5=10000
    # STATE=100 slope, intercept
    spike_intercept=250
    spike_slope=0.7
    autotune=true
    autotune_interval=240
    L=1000
    printf "" > workload_result.txt

    printf "RANGE\n" >> workload_result.txt
#    for RANGE_I in 2500; do # 7500 6250 5000 3750
#        L=700 #1000
#        run_one_exp
#        printf "${EXP_NAME}\n" >> workload_result.txt
#    done


    printf "PERIOD\n" >> workload_result.txt
    RANGE_I=5000
#    for PERIOD_I in 90; do # 60 90 180 30
#      if [[ "${PERIOD_I}" == 180 ]]; then
#        L=800
#        autotune_interval=240 # 240
#      else
#        L=1000 #2000
#        autotune_interval="$((${PERIOD_I}*2))" # 240
#      fi
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_result.txt
#    done
#    PERIOD_I=120
#    autotune_interval=240
#    L=1000

    printf "STATE\n" >> workload_result.txt
#    for STATE_SIZE2 in 40000; do #2500 5000 20000
#        STATE_SIZE3=${STATE_SIZE2}
#        STATE_SIZE4=${STATE_SIZE2}
#        if [[ ${STATE_SIZE2} == 2500 ]]; then
#          spike_slope=0.7
#          spike_intercept=150
#          L=700
#        fi
#        if [[ ${STATE_SIZE2} == 5000 ]]; then
#          spike_slope=0.7
#          spike_intercept=150
#          L=800
#        fi
#        if [[ ${STATE_SIZE2} == 20000 ]]; then
#          spike_slope=0.7
#          spike_intercept=500
#          L=1200
#        fi
#        if [[ ${STATE_SIZE2} == 40000 ]]; then
#          # intercept=180
#          spike_slope=0.7
#          spike_intercept=1300
#          L=3000
#        fi
#        run_one_exp
#        printf "${EXP_NAME}\n" >> workload_result.txt
#    done
#    STATE_SIZE2=10000
#    STATE_SIZE3=10000
#    STATE_SIZE4=10000
#    spike_intercept=250
#    spike_slope=0.7
#    L=1000

    printf "SKEW\n" >> workload_result.txt
    for ZIPF_SKEW in 0.1; do # 0.025 0.05 0.2 0.4
        L=700 #800
        run_one_exp
        printf "${EXP_NAME}\n" >> workload_result.txt
    done
    ZIPF_SKEW=0
    L=1000

    printf "TOPOLOGY\n" >> workload_result.txt
#    GRAPH=1op
#    vertex_id="a84740bacf923e828852cc4966f2247c" #,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5"
#    DELAY2=1000
#    L=500
#    LP2=63
#    P2=12
#    run_one_exp
#    printf "${EXP_NAME}\n" >> workload_result.txt
#    DELAY2=50
#    P2=1
#    L=1000
#
#    RANGE_I=5000
#    GRAPH=3op
#    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5"
#    DELAY3=50
#    DELAY4=1000
#    LP2=3
#    LP3=3
#    LP4=57
#    P2=1
#    P3=1
#    P4=12
#    for repeat in 1; do
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_result.txt
#    done

    GRAPH=1split3
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
    L=800
    LP5=3
    p5=1
    DELAY5=50
    run_one_exp
    printf "${EXP_NAME}\n" >> workload_result.txt
#    GRAPH=2split2
#    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
#    L=1000
#    LP5=3
#    p5=1
#    DELAY5=50
#    run_one_exp
#    printf "${EXP_NAME}\n" >> workload_result.txt


}

run_scale_test

