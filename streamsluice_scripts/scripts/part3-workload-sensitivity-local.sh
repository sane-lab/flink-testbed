#!/bin/bash

source config-systemsensitivity-local.sh

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
  EXP_NAME=workload-${setting}-${scaling_decision_option}-${how_conservative_flag}-${conservative_service_rate_flag}-${smooth_backlog_flag}-${SOURCE_TYPE}-${CURVE_TYPE}-${GRAPH}-${runtime}-${RATE1}-${TIME1}-${RATE2}-${RATE_I}-${TIME_I}-${P1}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}-${zipf_skew}-${L}-${migration_interval}-${epoch}-${decision_interval}-${is_treat}-${repeat}

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

  scaling_decision_option=1 #0

  is_treat=true
  repeat=1
  warmup=10000
  zipf_skew=0
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
    -zipf_skew ${zipf_skew} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -graph ${GRAPH} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -warmupTime ${warmupTime} -warmupRate ${warmupRate} \
    -source ${SOURCE_TYPE} -curve_type ${CURVE_TYPE} -run_time ${runtime} \
    -zipf_skew ${zipf_skew} &
}

run_scale_test(){
    is_scalein=true
    how_more_optimization_flag=false
    how_optimization_flag=false
    how_intrinsic_bound_flag=true
    how_conservative_flag=false # true
    coordination_latency_flag=true
    conservative_service_rate_flag=true # false
    smooth_backlog_flag=false
    new_metrics_retriever_flag=true

    autotune=true
    autotune_interval=60
    autotuner="UserLimitTuner"
    autotuner_latency_window=100
    autotuner_bar_lowerbound=350 #300
    autotuner_initial_value_option=4 # 1
    autotuner_adjustment_option=1
    autotuner_increase_bar_option=1 # 2
    autotuner_initial_value_alpha=1.2
    autotuner_adjustment_beta=2.0

    epoch=100
    decision_interval=1 #10
    snapshot_size=20
    L=1000 #2000 #2500
    migration_interval=1000 #500
    spike_slope=0.7
    autotuner_initial_value_option=5
    autotuner_increase_bar_option=8
    autotuner_increase_bar_alpha=0.2 #0.1 #0.25
    echo "Run micro bench workload sensitivity..."
    init

    # Different cases
    GRAPH="1split2join1"
    SOURCE_TYPE="when"
    CURVE_TYPE="mixed"
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
    autotune=false
    epoch=100
    decision_interval=1 #10
    snapshot_size=20


    L=2000 #1000 #2000 #2500
    migration_interval=1000 #2000 #3000

    STATE_SIZE2=5000
    STATE_SIZE3=5000
    STATE_SIZE4=5000
    STATE_SIZE5=5000
    runtime=720
    DELTA_I=270
    LP2=1
    LP3=1
    LP4=1
    LP5=28 #16

    printf "" > workload_sensitivity_result.txt

    # Setting 1
    printf "Setting 1\n" >> workload_sensitivity_result.txt
    setting="setting1"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=1500
    DELAY4=1000
    DELAY5=20
    STATE_SIZE2=10000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=10000
    STATE_SIZE4=10000
    STATE_SIZE5=10000
    LP2=1
    LP3=28 #1
    LP4=28
    LP5=1

    P2=1
    P3=17
    P4=17
    P5=1
    GRAPH="1split2join1"
    autotuner_bar_lowerbound=350
    CURVE_TYPE="sine"
    warmupRate=5000
    warmupTime=60
    RATE_I=5000
    TIME_I=20
    TIME1=25
    TIME2=25
    RATE1=7000
    RATE2=3000

    for CURVE_TYPE in "sine" "linear" "gradient"; do #
      is_treat=false
      autotune=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      for L in 2000; do
        for repeat in 2 3 4 5 6 7 8 9 10; do
          is_treat=true
          autotune=true
          how_type="streamsluice"
          run_one_exp
          printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
        done
      done
    done

    # Setting 2
    printf "Setting 2\n" >> workload_sensitivity_result.txt
    setting="setting2"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=1500
    DELAY4=1000
    DELAY5=20
    STATE_SIZE2=20000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=20000
    STATE_SIZE4=20000
    STATE_SIZE5=20000
    LP2=1
    LP3=28 #1
    LP4=28
    LP5=1

    P2=1
    P3=17
    P4=17
    P5=1
    GRAPH="1split2join1"
    autotuner_bar_lowerbound=350
    CURVE_TYPE="sine"
    warmupRate=5000
    warmupTime=60
    RATE_I=5000
    TIME_I=0
    TIME1=45
    TIME2=45
    RATE2=3500
    for RATE1 in 5500 6000 6500 7000 7500; do #
      is_treat=false
      autotune=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      for L in 2000; do #750 1000 1250
        for repeat in 2 3 4 5 6 7 8 9 10; do # 1
          is_treat=true
          autotune=true
          how_type="streamsluice"
          run_one_exp
          printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
        done
      done
    done

    # Setting 3
    printf "Setting 3\n" >> workload_sensitivity_result.txt
    setting="setting3"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=1500
    DELAY4=1000
    DELAY5=20
    STATE_SIZE2=20000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=20000
    STATE_SIZE4=20000
    STATE_SIZE5=20000
    LP2=1
    LP3=28 #1
    LP4=28
    LP5=1

    P2=1
    P3=17
    P4=17
    P5=1
    GRAPH="1split2join1"
    autotuner_bar_lowerbound=350
    CURVE_TYPE="sine" #"linear"
    warmupRate=5000
    warmupTime=60
    RATE_I=5000
    TIME_I=0
    RATE1=6500
    RATE2=3500
    for TIME1 in 15 30 45 60 75 150; do #
      TIME2=${TIME1}
      is_treat=false
      autotune=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      for repeat in 6 7 8 9 10; do # 1 2 3 4 5
        for L in 2000; do #250 500 750 1000 1250 1500
          is_treat=true
          autotune=true
          how_type="streamsluice"
          run_one_exp
          printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
        done
      done
    done

    # Setting 4
    printf "Setting 4\n" >> workload_sensitivity_result.txt
    setting="setting4"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=1500
    DELAY4=1000
    DELAY5=20
    STATE_SIZE2=20000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=20000
    STATE_SIZE4=20000
    STATE_SIZE5=20000
    LP2=1
    LP3=28 #1
    LP4=28
    LP5=1

    P2=1
    P3=17
    P4=17
    P5=1
    GRAPH="1split2join1"
    autotuner_bar_lowerbound=350
    CURVE_TYPE="sine" #"linear"
    warmupRate=5000
    warmupTime=60
    RATE_I=5000
    TIME_I=0
    RATE1=6500
    RATE2=3500
    TIME1=45
    TIME2=45
    GRAPH="1op"
    vertex_id="a84740bacf923e828852cc4966f2247c"
    LP2=28

    P2=17
    DELAY2=1500
    DELAY3=20
    DELAY4=20
    DELAY5=20
    is_treat=false
    autotune=false
    how_type="ds2"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
    for repeat in 6 7 8 9 10; do # 1 2 3 4 5
      for L in 2000; do #750 1000 1250
        is_treat=true
        autotune=true
        how_type="streamsluice"
        run_one_exp
        printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      done
    done

    GRAPH="2op"
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1"
    LP2=28
    LP3=28
    P2=17
    P3=17
    DELAY2=1500
    DELAY3=1000
    DELAY4=20
    DELAY5=20
    is_treat=false
    autotune=false
    how_type="ds2"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
    for repeat in 6 7 8 9 10; do # 1 2 3 4 5
      for L in 2000; do #750 1000 1250
        is_treat=true
        autotune=true
        how_type="streamsluice"
        run_one_exp
        printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      done
    done

    GRAPH="3op"
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5"
    LP2=28
    LP3=28
    LP4=1
    P2=17
    P3=17
    P4=1
    DELAY2=1500
    DELAY3=1000
    DELAY4=20
    DELAY5=20
    is_treat=false
    autotune=false
    how_type="ds2"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
    for repeat in 6 7 8 9 10; do # 1 2 3 4 5
      for L in 2000; do #750 1000 1250
        is_treat=true
        autotune=true
        how_type="streamsluice"
        run_one_exp
        printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      done
    done

    GRAPH="4op"
    LP2=28
    LP3=28
    LP4=1
    LP5=1
    P2=17
    P3=17
    P4=1
    P5=1
    DELAY2=1500
    DELAY3=1000
    DELAY4=20
    DELAY5=20
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
    is_treat=false
    autotune=false
    how_type="ds2"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
    for repeat in 6 7 8 9 10; do # 1 2 3 4 5
      for L in 2000; do #
        is_treat=true
        autotune=true
        how_type="streamsluice"
        run_one_exp
        printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      done
    done




    # Setting 5
    printf "Setting 5\n" >> workload_sensitivity_result.txt
    setting="setting5"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=1500
    DELAY4=1000
    DELAY5=20
    STATE_SIZE2=20000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=20000
    STATE_SIZE4=20000
    STATE_SIZE5=20000
    LP2=1
    LP3=28 #1
    LP4=28
    LP5=1

    P2=1
    P3=17
    P4=17
    P5=1
    GRAPH="1split2join1"
    vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
    autotuner_bar_lowerbound=350
    CURVE_TYPE="sine" #"linear"
    warmupRate=5000
    warmupTime=60
    RATE_I=5000
    TIME_I=0
    RATE1=6500
    RATE2=3500
    TIME1=45
    TIME2=45
    for DELAY4 in 2000 1333 1000 666 500; do # 1333 666
      P4=24
      is_treat=false
      autotune=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      for repeat in 6 7 8 9 10; do # 1 2 3 4 5
        for L in 2000; do #
          is_treat=true
          autotune=true
          how_type="streamsluice"
          run_one_exp
          printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
        done
      done
    done



    # Setting 6
    printf "Setting 6\n" >> workload_sensitivity_result.txt
    setting="setting6"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=1500
    DELAY4=1000
    DELAY5=20
    STATE_SIZE2=10000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=10000
    STATE_SIZE4=10000
    STATE_SIZE5=10000
    LP2=1
    LP3=28 #1
    LP4=28
    LP5=1

    P2=1
    P3=17
    P4=17
    P5=1
    GRAPH="1split2join1"
    autotuner_bar_lowerbound=350
    CURVE_TYPE="sine" #"linear"
    warmupRate=5000
    warmupTime=60
    RATE_I=5000
    TIME_I=0
    RATE1=6500
    RATE2=3500
    TIME1=45
    TIME2=45
    for STATE_SIZE5 in 0 5000 20000 40000 80000; do #
      STATE_SIZE2=${STATE_SIZE5}
      STATE_SIZE3=${STATE_SIZE5}
      STATE_SIZE4=${STATE_SIZE5}
      is_treat=false
      autotune=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      for L in 2000; do
        for repeat in 2 3 4 5 6 7 8 9 10; do
          is_treat=true
          autotune=true
          how_type="streamsluice"
          run_one_exp
          printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
        done
      done
    done

    # Setting 7
    printf "Setting 7\n" >> workload_sensitivity_result.txt
    setting="setting7"
    SOURCE_TYPE="when"
    DELAY2=20
    DELAY3=1500
    DELAY4=1000
    DELAY5=20
    STATE_SIZE2=20000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=20000
    STATE_SIZE4=20000
    STATE_SIZE5=20000
    LP2=1
    LP3=28 #1
    LP4=28
    LP5=1

    P2=1
    P3=17
    P4=17
    P5=1
    GRAPH="1split2join1"
    autotuner_bar_lowerbound=350
    CURVE_TYPE="sine" #"linear"
    warmupRate=5000
    warmupTime=60
    RATE_I=5000
    TIME_I=0
    RATE1=6500
    RATE2=3500
    TIME1=45
    TIME2=45
    for zipf_skew in 0.1 0.2 0.4 0.8; do # 0.1 0.2 0.4 0.8
      is_treat=false
      autotune=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      for repeat in 4 5 6 7 8 9 10; do # 1 2 3
        for L in 2000; do
          is_treat=true
          autotune=true
          how_type="streamsluice"
          run_one_exp
          printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
        done
      done
    done
}

run_scale_test

