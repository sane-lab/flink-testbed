#!/bin/bash

source config-systemsensitivity.sh

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

    for host in "dragon" "eagle"; do
      scp ${host}:${FLINK_DIR}/log/* ${EXP_DIR}/raw/${EXP_NAME}/
      ssh ${host} "rm ${FLINK_DIR}/log/*"
    done
}

run_one_exp() {
  EXP_NAME=system-${whether_type}-${how_type}-${how_conservative_flag}-${how_steady_limit_flag}-${how_optimization_flag}-${SOURCE_TYPE}-${CURVE_TYPE}-${GRAPH}-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}-${L}-${migration_interval}-${epoch}-${decision_interval}-${is_treat}-${repeat}

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

  P2=2
  MP2=128
  DELAY2=300
  IO2=1
  STATE_SIZE2=1000

  P3=2
  MP3=128
  DELAY3=300
  IO3=1
  STATE_SIZE3=1000

  P4=2
  MP4=128
  DELAY4=300
  IO4=1
  STATE_SIZE4=1000

  P5=6
  MP5=128
  DELAY5=1050 #510
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
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} \
    -source ${SOURCE_TYPE} -curve_type ${CURVE_TYPE} -run_time ${runtime} \
    -zipf_skew ${ZIPF_SKEW} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -graph ${GRAPH} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I}\
    -source ${SOURCE_TYPE} -curve_type ${CURVE_TYPE} -run_time ${runtime} \
    -zipf_skew ${ZIPF_SKEW} &
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
    SOURCE_TYPE="when"
    CURVE_TYPE="gradient"
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
    autotune=false
    epoch=100
    decision_interval=1 #10
    snapshot_size=3

    L=1000 #2000 #2500
    migration_interval=3000
    spike_slope=0.7

    STATE_SIZE2=5000
    STATE_SIZE3=5000
    STATE_SIZE4=5000
    STATE_SIZE5=5000
    spike_intercept=1000
    runtime=170 #520 #400
    DELTA_I=270
    LP2=2
    LP3=2
    LP4=2
    LP5=13

    RATE1=4000 #6000
    TIME1=90 #30
    RATE2=4000
    TIME2=30 #30
    RATE_I=4000
    TIME_I=60 #30

    # Curve 1
    SOURCE_TYPE="when"
    is_treat=true
    is_treat=false
    how_type="ds2"
    how_more_optimization_flag=false
    how_optimization_flag=false
    how_steady_limit_flag=true
    how_conservative_flag=true
    printf "" > whetherhow_result.txt

    # Different service rate
    P5=1
    LP5=1
#    for DELAY5 in 222 200 166 125 62; do
#      GRAPH="1op_line"
#      vertex_id="a84740bacf923e828852cc4966f2247c"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
#      GRAPH="8op_line"
#      vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47,36fcfcb61a35d065e60ee34fccb0541a,c395b989724fa728d0a2640c6ccdb8a1,8e0d1d377d577c52511ad507bf0ce330,2019e6125f7b4867f5fd448be51e3519"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    done

    # Different task number
    DELAY5=222
    for P5 in 2 4 8; do
      LP5=${P5}
      GRAPH="1op_line"
      vertex_id="a84740bacf923e828852cc4966f2247c"
      run_one_exp
      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    done

    # Different op number
#    DELAY5=222
#    P5=1
#    LP5=1
#    GRAPH="2op_line"
#    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    GRAPH="3op_line"
#    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    GRAPH="4op_line"
#    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    GRAPH="5op_line"
#    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47,36fcfcb61a35d065e60ee34fccb0541a"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    GRAPH="6op_line"
#    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47,36fcfcb61a35d065e60ee34fccb0541a,c395b989724fa728d0a2640c6ccdb8a1"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    GRAPH="7op_line"
#    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47,36fcfcb61a35d065e60ee34fccb0541a,c395b989724fa728d0a2640c6ccdb8a1,8e0d1d377d577c52511ad507bf0ce330"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
#    GRAPH="8op_line"
#    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47,36fcfcb61a35d065e60ee34fccb0541a,c395b989724fa728d0a2640c6ccdb8a1,8e0d1d377d577c52511ad507bf0ce330,2019e6125f7b4867f5fd448be51e3519"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt

    # TODO: Different arrival curve

    is_treat=true
}

run_scale_test

