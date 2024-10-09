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
  EXP_NAME=autotune-${setting}-${coordination_latency_flag}-${whether_type}-${how_type}-${autotune_interval}-${autotuner_latency_window}-${autotuner_bar_lowerbound}-${autotuner_adjustment_option}-${autotuner_adjustment_beta}-${autotuner_increase_bar_option}-${autotuner_increase_bar_alpha}-${SOURCE_TYPE}-${CURVE_TYPE}-${GRAPH}-${runtime}-${RATE1}-${TIME1}-${RATE2}-${RATE_I}-${TIME_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}-${L}-${migration_interval}-${epoch}-${decision_interval}-${is_treat}-${repeat}

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

function setting1(){
  # Setting 1
  printf "Setting 1\n" >> whetherhow_result.txt
  runtime=660
  setting="setting1"
  SOURCE_TYPE="when"
  autotuner_bar_lowerbound=300
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
  for TIME1 in 30; do # 60 45 30
    TIME2=${TIME1}
    is_treat=false
    how_type="ds2"
    autotune=false
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 500 1000 1500; do # 290 310 320 330 340 350 500 750 1000 1250 1500
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
            run_one_exp
            printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting2(){
  # Setting 2
  printf "Setting 2\n" >> whetherhow_result.txt
  runtime=660
  setting="setting2"
  SOURCE_TYPE="when"
  autotuner_bar_lowerbound=200
  DELAY2=20
  DELAY3=1000
  DELAY4=20
  DELAY5=500
  STATE_SIZE2=5000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
  STATE_SIZE3=5000
  STATE_SIZE4=5000
  STATE_SIZE5=5000
  LP2=1
  LP3=30
  P2=1
  P3=17
  P4=1
  P5=17
  GRAPH="2op_line"
  CURVE_TYPE="linear"
  warmupRate=10000
  warmupTime=60
  RATE_I=10000
  TIME_I=0
  RATE1=13750 #12500 #15000
  RATE2=6250 #7500 #5000
  for TIME1 in 30 20; do # 60 45 30
    TIME2=${TIME1}
    is_treat=false
    how_type="ds2"
    autotune=false
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 250 500 1000 1500; do # 290 310 320 330 340 350 500 750 1000 1250 1500
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
            run_one_exp
            printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting3(){
  # Setting 3
  printf "Setting 3\n" >> whetherhow_result.txt
  runtime=660
  setting="setting3"
  SOURCE_TYPE="when"
  autotuner_bar_lowerbound=400
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
  GRAPH="4op_line"
  CURVE_TYPE="gradient"
  warmupRate=10000
  warmupTime=60
  RATE_I=10000
  TIME_I=0
  RATE1=13750 #12500 #15000
  RATE2=6250 #7500 #5000
  for TIME1 in 30 20; do # 60 45 30
    TIME2=${TIME1}
    is_treat=false
    how_type="ds2"
    autotune=false
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 500 1000 1500 2000; do # 290 310 320 330 340 350 500 750 1000 1250 1500
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
            run_one_exp
            printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting4(){
  # Setting 4
  printf "Setting 4\n" >> whetherhow_result.txt
  runtime=660
  setting="setting4"
  SOURCE_TYPE="when"
  autotuner_bar_lowerbound=300
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
  RATE1=13750 #12500 #15000
  RATE2=6250 #7500 #5000
  for TIME1 in 30; do # 60 45 30
    TIME2=${TIME1}
    is_treat=false
    how_type="ds2"
    autotune=false
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 500 1000 1500; do # 290 310 320 330 340 350 500 750 1000 1250 1500
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
            run_one_exp
            printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting5(){
  # Setting 5
  printf "Setting 5\n" >> whetherhow_result.txt
  runtime=660
  setting="setting5"
  SOURCE_TYPE="when"
  autotuner_bar_lowerbound=300
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
  RATE1=13750 #12500 #15000
  RATE2=6250 #7500 #5000
  for TIME1 in 20; do # 60 45 30
    TIME2=${TIME1}
    is_treat=false
    how_type="ds2"
    autotune=false
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 500 1000 1500; do # 290 310 320 330 340 350 500 750 1000 1250 1500
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
            run_one_exp
            printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting6(){
  # Setting 6
  printf "Setting 6\n" >> whetherhow_result.txt
  runtime=660
  setting="setting6"
  SOURCE_TYPE="when"
  autotuner_bar_lowerbound=300
  DELAY2=20
  DELAY3=20
  DELAY4=20
  DELAY5=500
  STATE_SIZE2=20000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
  STATE_SIZE3=20000
  STATE_SIZE4=20000
  STATE_SIZE5=20000
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
  RATE1=13750 #12500 #15000
  RATE2=6250 #7500 #5000
  for TIME1 in 30; do # 60 45 30
    TIME2=${TIME1}
    is_treat=false
    how_type="ds2"
    autotune=false
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 5000 7500 10000; do # 500 1000 2500
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
            run_one_exp
            printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting7(){
  # Setting 7
  printf "Setting 7\n" >> whetherhow_result.txt
  runtime=1260
  setting="setting7"
  SOURCE_TYPE="changing_amplitude"
  autotuner_bar_lowerbound=300
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
  RATE1=14000 #12500 #15000
  RATE2=11000 #7500 #5000
  for TIME1 in 1200; do
    TIME2=60
    is_treat=false
    how_type="ds2"
    autotune=false
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 500 1000 1500 2000; do
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
            run_one_exp
            printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting8(){
  # Setting 8
  printf "Setting 8\n" >> whetherhow_result.txt
  runtime=1260
  setting="setting8"
  SOURCE_TYPE="changing_amplitude"
  autotuner_bar_lowerbound=300
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
  RATE1=14000 #12500 #15000
  RATE2=11000 #7500 #5000
  for TIME1 in 600; do
    TIME2=60
    is_treat=false
    how_type="ds2"
    autotune=false
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 500 1000 1500 2000; do
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
            run_one_exp
            printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting9(){
  # Setting 9
  printf "Setting 9\n" >> whetherhow_result.txt
  runtime=1260
  setting="setting9"
  SOURCE_TYPE="changing_period"
  autotuner_bar_lowerbound=300
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
    TIME2=30
    is_treat=false
    how_type="ds2"
    autotune=false
#      run_one_exp
#      printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 500 1000 1500 2000; do
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
          run_one_exp
          printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting10(){
  # Setting 10
  printf "Setting 10\n" >> whetherhow_result.txt
  runtime=660
  setting="setting10"
  SOURCE_TYPE="sine_with_spike"
  autotuner_bar_lowerbound=300
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
  RATE1=12500 #12500 #15000
  RATE2=17500 #7500 #5000
  TIME1=60
  for TIME2 in 2; do # 60 45 30
    is_treat=false
    how_type="ds2"
    autotune=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 500 1000 1500 2000; do
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
          run_one_exp
          printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting11(){
  # Setting 11
  printf "Setting 11\n" >> whetherhow_result.txt
  runtime=660
  setting="setting11"
  SOURCE_TYPE="sine_with_spike"
  autotuner_bar_lowerbound=300
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
  RATE1=12500 #12500 #15000
  RATE2=15000 #7500 #5000
  TIME1=60
  for TIME2 in 10; do # 60 45 30
    is_treat=false
    how_type="ds2"
    autotune=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 500 1000 1500 2000; do
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
          run_one_exp
          printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting12(){
  # Setting 12
  printf "Setting 12\n" >> whetherhow_result.txt
  runtime=1260
  setting="setting12"
  SOURCE_TYPE="sine_shift"
  autotuner_bar_lowerbound=300
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
  RATE1=12500 #12500 #15000
  RATE2=15000 #7500 #5000
  TIME1=60
  for TIME2 in 40; do # 60 45 30
    is_treat=false
    how_type="ds2"
    autotune=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 500 1000 1500 2000; do
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
          run_one_exp
          printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting13(){
  # Setting 13
  printf "Setting 13\n" >> whetherhow_result.txt
  runtime=1860
  setting="setting13"
  SOURCE_TYPE="linear_phase_change"
  autotuner_bar_lowerbound=300
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
  RATE1=12500 #12500 #15000
  RATE2=15000 #7500 #5000
  TIME1=360
  TIME2=40
  for STATE_SIZE2 in 5000; do # 10000
    STATE_SIZE3=${STATE_SIZE2}
    STATE_SIZE4=${STATE_SIZE2}
    STATE_SIZE5=${STATE_SIZE2}
    for parameter_index in 2; do # 1 3
      if [ "$parameter_index" = 1 ]; then
        RATE_I=5000
        RATE1=10000
        TIME1=360
      elif [ "$parameter_index" = 2 ]; then
        RATE_I=5000
        RATE1=12500
        TIME1=900
      elif [ "$parameter_index" = 3 ]; then
        RATE_I=7500
        RATE1=12500
        TIME1=180
      fi
      is_treat=false
      how_type="ds2"
      autotune=false
  #    run_one_exp
  #    printf "${EXP_NAME}\n" >> whetherhow_result.txt
      for L in 5000 10000 15000 20000 25000; do # 1000 1500 2000 10000 20000
        is_treat=true
        how_type="streamsluice"
        autotune=true
        for autotuner_initial_value_option in 2; do
          if [ "$autotuner_initial_value_option" = 1 ]; then
            autotuner_initial_value_alpha=0.5
          elif [ "$autotuner_initial_value_option" = 2 ]; then
            autotuner_initial_value_alpha=0.2
          fi
          for autotuner_adjustment_option in 1; do
            if [ "$autotuner_adjustment_option" = 1 ]; then
              autotuner_adjustment_beta=2.0
            elif [ "$autotuner_adjustment_option" = 2 ]; then
              autotuner_adjustment_beta=1.0
            fi
            run_one_exp
            printf "${EXP_NAME}\n" >> whetherhow_result.txt
          done
        done
      done
    done
  done
}

function setting14(){
  # Setting 14
  printf "Setting 14\n" >> whetherhow_result.txt
  runtime=660
  setting="setting14"
  SOURCE_TYPE="sine_two_phase"
  autotuner_bar_lowerbound=300
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
  RATE1=12500 #12500 #15000
  RATE2=10000 #7500 #5000
  TIME1=60
  TIME2=60
  for RATE1 in 12500; do # 60 45 30
    is_treat=false
    how_type="ds2"
    autotune=false
#    run_one_exp
#    printf "${EXP_NAME}\n" >> whetherhow_result.txt
    for L in 4000 5000 10000; do # 500 1000 1500
      is_treat=true
      how_type="streamsluice"
      autotune=true
      for autotuner_initial_value_option in 2; do
        if [ "$autotuner_initial_value_option" = 1 ]; then
          autotuner_initial_value_alpha=0.5
        elif [ "$autotuner_initial_value_option" = 2 ]; then
          autotuner_initial_value_alpha=0.2
        fi
        for autotuner_adjustment_option in 1; do
          if [ "$autotuner_adjustment_option" = 1 ]; then
            autotuner_adjustment_beta=2.0
          elif [ "$autotuner_adjustment_option" = 2 ]; then
            autotuner_adjustment_beta=1.0
          fi
          run_one_exp
          printf "${EXP_NAME}\n" >> whetherhow_result.txt
        done
      done
    done
  done
}

function setting15(){
  # Setting 15
  printf "Setting 15\n" >> whetherhow_result.txt
  runtime=1260
  setting="setting15"
  SOURCE_TYPE="changing_amplitude_and_period"
  autotuner_bar_lowerbound=300
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
  RATE1=14000 #15000
  RATE2=10000 #5000
  TIME1=60
  for TIME2 in 90; do
    for RATE2 in 10000; do
      is_treat=false
      how_type="ds2"
      autotune=false
  #      run_one_exp
  #      printf "${EXP_NAME}\n" >> whetherhow_result.txt
      for L in 4000 5000 7500 10000 15000 20000; do # 500 1000 1500 2000
        is_treat=true
        how_type="streamsluice"
        autotune=true
        for autotuner_initial_value_option in 2; do
          if [ "$autotuner_initial_value_option" = 1 ]; then
            autotuner_initial_value_alpha=0.5
          elif [ "$autotuner_initial_value_option" = 2 ]; then
            autotuner_initial_value_alpha=0.2
          fi
          for autotuner_adjustment_option in 1; do
            if [ "$autotuner_adjustment_option" = 1 ]; then
              autotuner_adjustment_beta=2.0
            elif [ "$autotuner_adjustment_option" = 2 ]; then
              autotuner_adjustment_beta=1.0
            fi
            run_one_exp
            printf "${EXP_NAME}\n" >> whetherhow_result.txt
          done
        done
      done
    done
  done
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
    autotune_interval=60 #30
    autotuner="UserLimitTuner"
    autotuner_latency_window=100
    autotuner_bar_lowerbound=300
    autotuner_initial_value_option=1
    autotuner_adjustment_option=1
    autotuner_increase_bar_option=1 # 2
    autotuner_initial_value_alpha=1.2
    autotuner_adjustment_beta=2.0
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
    how_intrinsic_bound_flag=true
    how_conservative_flag=false # true
    coordination_latency_flag=true
    conservative_service_rate_flag=true # false
    smooth_backlog_flag=false
    new_metrics_retriever_flag=true

    runtime=660 #390
    #setting1
    #setting2
    #setting3
    #setting4
    #setting5
    #setting6
    #setting7
    #setting8
    #setting9
    #setting10
    #setting11
    #setting12
    #setting13

    autotuner_increase_bar_option=7 # 3 5
    autotuner_increase_bar_alpha=0.1 #0.25
    autotune_interval=90
    setting14
    setting15
    setting13
}

run_scale_test

