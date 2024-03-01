#!/bin/bash

source config-server-systemsensitivity.sh

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

    for host in "dragon" "flamingo"; do
      scp ${host}:${FLINK_DIR}/log/* ${EXP_DIR}/raw/${EXP_NAME}/
      ssh ${host} "rm ${FLINK_DIR}/log/*"
    done
}

run_one_exp() {
  EXP_NAME=microbench-system-server-${whether_type}-${how_type}-${CURVE_TYPE}-${GRAPH}-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${RANGE_I}-${PERIOD_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}-${L}-${migration_interval}-${epoch}-${decision_interval}-${is_treat}-${repeat}

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

  P4=3
  MP4=128
  DELAY4=444
  IO4=1
  STATE_SIZE4=1000

  P5=5
  MP5=128
  DELAY5=500
  STATE_SIZE5=1000

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
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
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
    GRAPH="1split2join1"
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
    autotune=false
    epoch=100
    decision_interval=10
    snapshot_size=3

    L=2000
    migration_interval=2000
    spike_slope=0.7
    #spike_intercept=200
    #STATE_SIZE2=1000
    #STATE_SIZE3=1000
    #STATE_SIZE4=1000
    spike_intercept=1400 #1200 #750

    STATE_SIZE2=10000
    STATE_SIZE3=10000
    STATE_SIZE4=10000
    STATE_SIZE5=10000
    runtime=60
    DELTA_I=270
    LP2=5
    LP3=5
    LP4=5
    LP5=13
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

#    whether_type="streamsluice"
#    how_type="streamsluice"
#    whether_type="time_15"
    whether_type="streamsluice"
    how_type="op_2_3_keep"
    is_treat=true
    printf "1_${CURVE_TYPE}\n" >> whetherhow_result.txt
#    is_treat=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    is_treat=true
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    whether_early="streamsluice_earlier"
    whether_late="streamsluice_later"
    for whether_type in ${whether_early} ${whether_late}; do
      #how_type="streamsluice"
      run_one_exp
      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    done

#    whether_type="streamsluice"
#    how_type="streamsluice"
#    #whether_type="time_170"
#    #for how_type in "op_1_6_keep" "op_1_1_keep" "op_2_2_keep"; do
#    for how_type in "streamsluice_no_balance" "streamsluice_minus_one" "streamsluice_more" "streamsluice_less" "streamsluice_not_bottleneck"; do
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done


    # Curve 2
    CURVE_TYPE="gradient"
    RATE_I=4750
    RANGE_I=750
    RATE2=5500
    TIME1=30
    PERIOD_I=20
    TIME_I=10
#    whether_type="streamsluice"
#    how_type="streamsluice"
    whether_type="streamsluice"
    how_type="op_2_3_keep"
    is_treat=true
    printf "2_${CURVE_TYPE}\n" >> whetherhow_result.txt
#    is_treat=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    is_treat=true
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    whether_early="streamsluice_earlier"
    whether_late="streamsluice_later"
    for whether_type in ${whether_early}; do #${whether_late}
      #how_type="streamsluice"
      run_one_exp
      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    done


#    whether_type="streamsluice"
#    how_type="streamsluice"
#    whether_type="time_190" # "time_180" for 3op
#    #for how_type in "op_1_6_keep" "op_1_1_keep" "op_2_2_keep"; do
#    for how_type in "streamsluice_less"; do # "streamsluice_no_balance" "streamsluice_minus_one"   "streamsluice_more"  "streamsluice_not_bottleneck"; do
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done


    # Curve 3
    CURVE_TYPE="sine"
    RATE1=4000
    TIME1=30
    RATE2=6000
    TIME2=120
    RATE_I=5000
    RANGE_I=1000
    PERIOD_I=20
    TIME_I=10
#    whether_type="streamsluice"
#    how_type="streamsluice"
    whether_type="streamsluice"
    how_type="op_2_3_keep"
    printf "3_${CURVE_TYPE}\n" >> whetherhow_result.txt
#    is_treat=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    is_treat=true
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    whether_early="streamsluice_earlier"
    whether_late="streamsluice_later"
    for whether_type in ${whether_early}; do # ${whether_late}
      #how_type="streamsluice"
      run_one_exp
      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    done

#    whether_type="streamsluice"
#    how_type="streamsluice"
#    whether_type="time_230"
##    for how_type in "op_1_6_keep" "op_1_1_keep" "op_2_2_keep"; do
#    for how_type in "streamsluice_minus_one" "streamsluice_less"; do # "streamsluice_more"  "streamsluice_no_balance" "streamsluice_not_bottleneck"; do
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done


    # Curve 4
    CURVE_TYPE="gradient"
#    whether_type="streamsluice"
#    how_type="streamsluice"
    whether_type="streamsluice"
    how_type="op_2_3_keep"
    RATE1=4000
    RATE2=4000
    RATE_I=5000
    RANGE_I=1000
    PERIOD_I=10
    TIME_I=10
    printf "4_${CURVE_TYPE}\n" >> whetherhow_result.txt
#    is_treat=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    is_treat=true
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    whether_early="streamsluice_earlier"
    whether_late="streamsluice_later"
    for whether_type in ${whether_early} ${whether_late}; do
      #how_type="streamsluice"
      run_one_exp
      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    done


#    whether_type="streamsluice"
#    how_type="streamsluice"
#    whether_type="time_170"
##    for how_type in "op_1_6_keep" "op_1_1_keep" "op_2_2_keep"; do
#    for how_type in "streamsluice_no_balance" "streamsluice_minus_one" "streamsluice_more" "streamsluice_less" "streamsluice_not_bottleneck"; do
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done

    # Curve 5
    CURVE_TYPE="gradient"
    whether_type="streamsluice"
    how_type="streamsluice"
    RATE1=4000
    RATE2=4000
    RATE_I=5000
    RANGE_I=1000
    TIME1=32
    PERIOD_I=2
    TIME_I=2
    printf "5_${CURVE_TYPE}\n" >> whetherhow_result.txt
#    is_treat=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    is_treat=true
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt

#    whether_early="streamsluice_earlier"
#    for whether_type in ${whether_early} ; do
##      how_type="streamsluice"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    done
}

run_scale_test

