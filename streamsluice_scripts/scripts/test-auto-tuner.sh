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
  EXP_NAME=autotune-${setting}-${coordination_latency_flag}-${whether_type}-${how_type}-${autotuner_latency_window}-${autotuner_bar_lowerbound}-${autotuner_initial_value_option}-${autotuner_initial_value_alpha}-${autotuner_adjustment_option}-${autotuner_adjustment_alpha}-${autotuner_increase_bar_option}-${SOURCE_TYPE}-${CURVE_TYPE}-${GRAPH}-${runtime}-${RATE1}-${TIME1}-${RATE2}-${RATE_I}-${TIME_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}-${L}-${migration_interval}-${epoch}-${decision_interval}-${is_treat}-${repeat}

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
  DELAY5=510
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
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -warmupTime ${warmupTime} -warmupRate ${warmupRate} \
    -source ${SOURCE_TYPE} -curve_type ${CURVE_TYPE} -run_time ${runtime} \
    -zipf_skew ${ZIPF_SKEW} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -graph ${GRAPH} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -warmupTime ${warmupTime} -warmupRate ${warmupRate} \
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
    CURVE_TYPE="mixed"
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
    autotune=true
    autotuner="UserLimitTuner"
    autotuner_latency_window=100
    autotuner_bar_lowerbound=300
    autotuner_initial_value_option=1
    autotuner_adjustment_option=1
    autotuner_increase_bar_option=2
    autotuner_initial_value_alpha=1.2
    autotuner_adjustment_alpha=2.0
    epoch=100
    decision_interval=1 #10
    snapshot_size=20

    L=1000 #2000 #2500
    migration_interval=3000
    spike_slope=0.7
    #spike_intercept=200
    #STATE_SIZE2=1000
    #STATE_SIZE3=1000
    #STATE_SIZE4=1000
    #spike_intercept=1500 #1200 #750

#    STATE_SIZE2=10000
#    STATE_SIZE3=10000
#    STATE_SIZE4=10000
#    STATE_SIZE5=10000
    STATE_SIZE2=5000
    STATE_SIZE3=5000
    STATE_SIZE4=5000
    STATE_SIZE5=5000
    spike_intercept=1000
    runtime=520 #520 #400
    DELTA_I=270
    LP2=1
    LP3=1
    LP4=1
    LP5=28 #16

    RATE1=6000
    TIME1=30
    RATE2=3000
    TIME2=30
    RATE_I=4000
    TIME_I=30
    printf "" > whetherhow_result.txt
    how_more_optimization_flag=false
    how_optimization_flag=false
    how_steady_limit_flag=true
    how_conservative_flag=false # true
    coordination_latency_flag=true
    conservative_service_rate_flag=true # false
    smooth_backlog_flag=false
    new_metrics_retriever_flag=true



    runtime=390
    # Setting 1
    printf "Setting 1\n" >> whetherhow_result.txt
    setting="setting1"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=20
    DELAY4=20
    DELAY5=1000
    STATE_SIZE2=5000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=5000
    STATE_SIZE4=5000
    STATE_SIZE5=5000
    LP2=1
    LP3=1
    LP4=1
    LP5=28
    P2=1
    P3=1
    P4=1
    P5=17
    CURVE_TYPE="linear"
    RATE1=10000
    TIME1=600
    RATE_I=10000
    TIME_I=90
    RATE2=10000
    TIME2=600
    warmupRate=${RATE_I}
    warmupTime=${TIME_I}
    for GRAPH in "1op_line" "2op_line" "3op_line"; do
      is_treat=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
      # Set the initial value of L based on the value of GRAPH
      if [ "$GRAPH" = "1op_line" ]; then
        for L in 90 125 250 500 750 1250 1500; do # 90 110 120 130 140 150 250 500 750 1000 1250 1500
          LP2=31
          is_treat=true
          how_type="streamsluice"
#          run_one_exp
#          printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      elif [ "$GRAPH" = "2op_line" ]; then
        LP2=1
        LP3=30
        for L in 190 225 500 750 1250 1500; do # 190 210 220 230 240 250 500 750 1000 1250 1500
          is_treat=true
          how_type="streamsluice"
#          run_one_exp
#          printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      elif [ "$GRAPH" = "3op_line" ]; then
        LP2=1
        LP3=1
        LP4=29
        for L in 290 325 500 750 1250 1500; do # 290 310 320 330 340 350 500 750 1000 1250 1500
          is_treat=true
          how_type="streamsluice"
#          run_one_exp
#          printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      fi

    done

    # Setting 2:
    printf "Setting 2\n" >> whetherhow_result.txt
    setting="setting2"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=20
    DELAY4=20
    DELAY5=1000
    STATE_SIZE2=5000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=5000
    STATE_SIZE4=5000
    STATE_SIZE5=5000
    LP2=1
    LP3=1
    LP4=29

    P2=1
    P3=1
    P4=1
    P5=17
    GRAPH="3op_line"
    CURVE_TYPE="linear"
    RATE1=10000
    TIME1=600
    RATE_I=5000
    TIME_I=0
    RATE2=10000
    TIME2=600
    warmupRate=5000
    warmupTime=60
    for RATE1 in 10000 15000 20000; do
      is_treat=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
      for L in 290 325 500 750 1250 1500; do # 290 310 320 330 340 350 500 750 1000 1250 1500
        is_treat=true
        how_type="streamsluice"
#        run_one_exp
#        printf "${EXP_NAME}\n" >> whetherhow_result.txt
      done
    done

    # Setting 3
    printf "Setting 3\n" >> whetherhow_result.txt
    setting="setting3"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=20
    DELAY4=20
    DELAY5=500
    STATE_SIZE2=5000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=5000
    STATE_SIZE4=5000
    STATE_SIZE5=5000
    LP2=1
    LP3=1
    LP4=1
    LP5=28

    P2=1
    P3=1
    P4=1
    P5=17
    GRAPH="1split2join1"
    CURVE_TYPE="sine" #"linear"
    warmupRate=10000
    warmupTime=60
    RATE_I=10000
    TIME_I=0
    RATE1=12500 #15000
    RATE2=7500 #5000
    for TIME1 in 60; do # 60 45 30
      TIME2=${TIME1}
      is_treat=false
      how_type="ds2"
      run_one_exp
      printf "${EXP_NAME}\n" >> whetherhow_result.txt
      for L in 1000; do # 290 310 320 330 340 350 500 750 1000 1250 1500
        is_treat=true
        how_type="streamsluice"
        run_one_exp
        printf "${EXP_NAME}\n" >> whetherhow_result.txt
      done
    done

    # Setting 4
    printf "Setting 4\n" >> whetherhow_result.txt
    setting="setting4"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=20
    DELAY4=1000
    DELAY5=20
    STATE_SIZE2=5000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=5000
    STATE_SIZE4=5000
    STATE_SIZE5=5000
    LP2=1
    LP3=1
    LP4=28
    LP5=1

    P2=1
    P3=1
    P4=17
    P5=1
    GRAPH="1split2join1"
    CURVE_TYPE="sine"
    warmupRate=10000
    warmupTime=60
    RATE_I=10000
    TIME_I=0
    TIME1=30
    TIME2=30
    for RATE1 in 13750; do
      is_treat=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
      for L in 350 500 750 1000; do # 290 310 320 330 340 350 500 750 1000 1250 1500
        is_treat=true
        how_type="streamsluice"
#        run_one_exp
#        printf "${EXP_NAME}\n" >> whetherhow_result.txt
      done
    done

    # Setting 5
    printf "Setting 5\n" >> whetherhow_result.txt
    setting="setting5"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=20
    DELAY4=20
    DELAY5=500
    STATE_SIZE2=5000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=5000
    STATE_SIZE4=5000
    STATE_SIZE5=5000
    LP2=1
    LP3=1
    LP4=1
    LP5=28

    P2=1
    P3=1
    P4=1
    P5=17
    GRAPH="1split2join1"
    CURVE_TYPE="gradient"
    warmupRate=10000
    warmupTime=60
    RATE_I=10000
    TIME_I=0
    RATE1=15000
    RATE2=5000
    TIME1=45
    TIME2=45
    for RATE1 in 13750; do #12500 15000 17500
      RATE2=$((20000 - RATE1))
      is_treat=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
      for L in 350 500 750 1000; do # 290 310 320 330 340 350 500 750 1000 1250 1500
        is_treat=true
        how_type="streamsluice"
#        run_one_exp
#        printf "${EXP_NAME}\n" >> whetherhow_result.txt
      done
    done
}

run_scale_test

