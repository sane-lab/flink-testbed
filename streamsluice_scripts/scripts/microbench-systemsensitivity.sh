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
  EXP_NAME=microbench-system-${whether_type}-${how_type}-${CURVE_TYPE}-${GRAPH}-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${RANGE_I}-${PERIOD_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${L}-${migration_interval}-${epoch}-${is_treat}-${repeat}

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
  whether_type="streamsluice"
  how_type="streamsluice"
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5"
  L=1000
  migration_interval=500
  epoch=100
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StreamSluiceTestSet.MicroBench"
  # only used in script
  runtime=300
  # set in Flink app
  GRAPH=3op
  ZIPF_SKEW=0
  NKEYS=1000
  P1=1
  MP1=1

  P2=3
  MP2=128
  DELAY2=444
  IO2=1
  STATE_SIZE2=1000

  P3=3
  MP3=128
  DELAY3=444
  IO3=1
  STATE_SIZE3=1000

  P4=5
  MP4=128
  DELAY4=1000 #500
  IO4=1
  STATE_SIZE4=1000

  spike_estimation="linear_regression"
  spike_slope=0.65
  spike_intercept=250
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
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interRange ${RANGE_I} -interPeriod ${PERIOD_I} -inter_delta ${DELTA_I} \
    -zipf_skew ${ZIPF_SKEW} -curve_type ${CURVE_TYPE} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -graph ${GRAPH} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interRange ${RANGE_I} -interPeriod ${PERIOD_I} -inter_delta ${DELTA_I} \
    -zipf_skew ${ZIPF_SKEW} -curve_type ${CURVE_TYPE} &
}

run_scale_test(){
    echo "Run micro bench system sensitivity..."
    init
    #L=1000
    #is_treat=false
    #repeat=1
    #run_one_exp


    # Different cases
    GRAPH=3op
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5"
    autotune=false

    L=2000
    migration_interval=2000
    spike_slope=0.65
    #spike_intercept=200
    #STATE_SIZE2=1000
    #STATE_SIZE3=1000
    #STATE_SIZE4=1000
    spike_intercept=500
    STATE_SIZE2=10000
    STATE_SIZE3=10000
    STATE_SIZE4=10000
    runtime=60
    DELTA_I=270
    LP2=5
    LP3=5
    LP4=13
    TIME_I=10
    RATE1=4000
    TIME1=30
    RATE2=6000
    TIME2=120
    RATE_I=5000
    RANGE_I=1000
    PERIOD_I=20
    TIME_I=10
    printf "" > whetherhow_result.txt

    # Curve 1
    CURVE_TYPE="gradient"

    whether_type="streamsluice"
    how_type="streamsluice"
    is_treat=true
    printf "1_${CURVE_TYPE}\n" >> whetherhow_result.txt
#    is_treat=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    is_treat=true
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    # time 16.3 5->8   statesize=1000
#    whether_early="time_14" # "time_14"
#    whether_late="time_18" # "time_18"
#    for whether_type in ${whether_early} ${whether_late}; do
#      how_type="streamsluice"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done
#    # time 15.6 5->8   statesize=10000
#    whether_early="time_14" # "time_14"
#    whether_late="time_17" # "time_18"
#    for whether_type in ${whether_early} ${whether_late}; do
#      how_type="streamsluice"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done

#    whether_type="streamsluice"
#    how_type="streamsluice"
#    for how_type in "op_1_6_keep" "op_1_1_keep" "op_2_2_keep"; do
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done





    # Curve 2
    CURVE_TYPE="sine"
    whether_type="streamsluice"
    how_type="streamsluice"
    printf "2_${CURVE_TYPE}\n" >> whetherhow_result.txt
#    is_treat=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    is_treat=true
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    # time 23.1 scale-out 5->8   statesize=1000
#    whether_early="time_21"   #"time_21"
#    whether_late="time_25"    #"time_25"
#    for whether_type in ${whether_early} ${whether_late}; do
#      how_type="streamsluice"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done
#    # time 21.7 scale-out 5->8   statesize=10000
#    whether_early="time_20"   #"time_21"
#    whether_late="time_23"    #"time_25"
#    for whether_type in ${whether_early} ${whether_late}; do
#      how_type="streamsluice"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done

#    whether_type="streamsluice"
#    how_type="streamsluice"
#    for how_type in "op_1_6_keep" "op_1_1_keep" "op_2_2_keep"; do
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done


    # Curve 3
    CURVE_TYPE="gradient"
    whether_type="streamsluice"
    how_type="streamsluice"
    RATE1=4000
    RATE2=4000
    RATE_I=5000
    RANGE_I=1000
    PERIOD_I=10
    TIME_I=10
    printf "3_${CURVE_TYPE}\n" >> whetherhow_result.txt
    is_treat=false
    run_one_exp
    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    is_treat=true
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    # time 15.8 5->8  statesize=1000
#    whether_early="time_14" # "time_14"
#    whether_late="time_18" # "time_18"
#    for whether_type in ${whether_early} ${whether_late}; do
#      how_type="streamsluice"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done
    # time 15 5->8  statesize=10000
    whether_early="time_13"
    whether_late="time_17"
    for whether_type in ${whether_early} ${whether_late}; do
      how_type="streamsluice"
      run_one_exp
      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    done


#    whether_type="streamsluice"
#    how_type="streamsluice"
#    for how_type in "op_1_6_keep" "op_1_1_keep" "op_2_2_keep"; do
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done

    # Curve 4
    CURVE_TYPE="sine"
    whether_type="streamsluice"
    how_type="streamsluice"
    RATE1=4000
    RATE2=4000
    RATE_I=5000
    RANGE_I=1000
    TIME1=32
    PERIOD_I=6
    TIME_I=6
    printf "4_${CURVE_TYPE}\n" >> whetherhow_result.txt
#    is_treat=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    is_treat=true
    run_one_exp
    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    # time 16.9 5->8  #old time 15.9 5->8
#    whether_early="time_21" # "time_14"
#    for whether_type in ${whether_early} ; do
#      how_type="streamsluice"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done
}

run_scale_test

