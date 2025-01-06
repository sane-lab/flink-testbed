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
  EXP_NAME=part6and7-${setting}-${whether_type}-${how_type}-${SOURCE_TYPE}-${CURVE_TYPE}-${GRAPH}-${runtime}-${rate_low}-${rate_high}-${rate_period}-${rate_pattern}-${amplitude_low}-${amplitude_high}-${amplitude_period}-${amplitude_pattern}-${period_low}-${period_high}-${period_period}-${period_pattern}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}--${noise}-${autotuner_increase_bar_alpha}-${L}-${migration_interval}-${epoch}-${decision_interval}-${is_treat}-${repeat}
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
  ZIPF_SKEW=0
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
    -zipf_skew ${ZIPF_SKEW} \
    -rateLow ${rate_low} -rateHigh ${rate_high} -ratePeriod ${rate_period} -ratePattern ${rate_pattern} \
    -amplitudeLow ${amplitude_low} -amplitudeHigh ${amplitude_high} -amplitudePeriod ${amplitude_period} -amplitudePattern ${amplitude_pattern} \
    -periodLow ${period_low} -periodHigh ${period_high} -periodPeriod ${period_period} -periodPattern ${period_pattern} \
    -noise ${noise} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -graph ${GRAPH} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -warmupTime ${warmupTime} -warmupRate ${warmupRate} \
    -source ${SOURCE_TYPE} -curve_type ${CURVE_TYPE} -run_time ${runtime} \
    -zipf_skew ${ZIPF_SKEW} \
    -rateLow ${rate_low} -rateHigh ${rate_high} -ratePeriod ${rate_period} -ratePattern ${rate_pattern} \
    -amplitudeLow ${amplitude_low} -amplitudeHigh ${amplitude_high} -amplitudePeriod ${amplitude_period} -amplitudePattern ${amplitude_pattern} \
    -periodLow ${period_low} -periodHigh ${period_high} -periodPeriod ${period_period} -periodPattern ${period_pattern} \
    -noise ${noise} &
}

run_scale_test(){
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
    autotuner_increase_bar_option=7 # 3 5
    autotuner_increase_bar_alpha=0.1 #0.25
    echo "Run micro bench whether"
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


    L=1000 #2000 #2500
    migration_interval=3000

    STATE_SIZE2=5000
    STATE_SIZE3=5000
    STATE_SIZE4=5000
    STATE_SIZE5=5000
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
    printf "" > part6and7_result.txt

    runtime=570

    # Period Amplitude Change
    printf "MicroBench\n" >> part6and7_result.txt
    setting="microbench"
    SOURCE_TYPE="part6"
    DELAY2=20
    DELAY3=50 #250 #333
    DELAY4=20
    DELAY5=800 #800
    STATE_SIZE2=1 #5000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=1 #5000
    STATE_SIZE4=1 #5000
    STATE_SIZE5=1 #5000
    LP2=1
    LP3=6 #9
    LP4=1
    LP5=31 #28

    P2=1
    P3=2 #3
    P4=1
    P5=17
    GRAPH="1split2join1"
    autotuner_bar_lowerbound=350
    autotuner_latency_window=100
    autotuner_increase_bar_alpha=0.1
    epoch=100
    CURVE_TYPE="mixed" #"linear"
    warmupRate=5000
    warmupTime=60
    rate_low=5000
    rate_high=5000
    rate_period=960
    rate_pattern="linear"
    amplitude_low=2000
    amplitude_high=1
    amplitude_period=1440
    amplitude_pattern="stair_3"
    period_low=120
    period_high=1
    period_period=1440
    period_pattern="stair_3"
    noise=0.05

    is_treat=false
    autotune=false
    how_type="ds2"
#    run_one_exp
#    printf "${EXP_NAME}\n" >> part6and7_result.txt
    how_type="streamsluice"
    for L in 1000; do # 2000 3000
      for whether_type in "streamsluice" "ds2" "dhalion" "streamswitch"; do #
        for repeat in 1; do
          is_treat=true
          autotune=false
#          run_one_exp
#          printf "${EXP_NAME}\n" >> part6and7_result.txt
        done
      done
    done

    SOURCE_TYPE="part7"
    whether_type="streamsluice"
    for L in 1000; do # 2000 3000
      for how_type in "drs"; do # "ds2" "drs" "streamswitch"
        for repeat in 1 2 3; do
          is_treat=true
          autotune=false
          run_one_exp
          printf "${EXP_NAME}\n" >> part6and7_result.txt
        done
      done
    done
}

run_scale_test

