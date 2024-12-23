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

    for host in "dragon"; do # "eagle"
      scp ${host}:${FLINK_DIR}/log/* ${EXP_DIR}/raw/${EXP_NAME}/
      ssh ${host} "rm ${FLINK_DIR}/log/*"
    done
}

run_one_exp() {
  EXP_NAME=${setting}-${scaling_decision_option}-${whether_type}-${how_type}-${how_conservative_flag}-${conservative_service_rate_flag}-${smooth_backlog_flag}-${SOURCE_TYPE}-${CURVE_TYPE}-${GRAPH}-${runtime}-${RATE1}-${TIME1}-${RATE2}-${RATE_I}-${TIME_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}-${L}-${migration_interval}-${epoch}-${autotune_interval}-${autotuner_increase_bar_alpha}-${decision_interval}-${is_treat}-${repeat}

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
  runtime=900
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
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -warmupTime ${warmupTime} -warmupRate ${warmupRate} \
    -source ${SOURCE_TYPE} -curve_type ${CURVE_TYPE} -run_time ${runtime} \
    -amplitudeLow ${amplitude_low} -amplitudeHigh ${amplitude_high} \
    -periodLow ${period_low} -periodHigh ${period_high} -stairs ${STAIRS} \
    -noise ${noise} -zipf_skew ${ZIPF_SKEW} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -graph ${GRAPH} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -warmupTime ${warmupTime} -warmupRate ${warmupRate} \
    -source ${SOURCE_TYPE} -curve_type ${CURVE_TYPE} -run_time ${runtime} \
    -amplitudeLow ${amplitude_low} -amplitudeHigh ${amplitude_high} \
    -periodLow ${period_low} -periodHigh ${period_high} -stairs ${STAIRS} \
    -noise ${noise} -zipf_skew ${ZIPF_SKEW} &
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

    noise=0.05
    epoch=100
    decision_interval=1 #10
    snapshot_size=20
    L=1000 #2000 #2500
    migration_interval=1000 #500
    spike_slope=0.7
    autotuner_initial_value_option=5
    autotuner_increase_bar_option=8
    autotuner_increase_bar_alpha=0.1 #0.25

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
    echo "Run micro bench system sensitivity..."
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
    migration_interval=500 #1000

    printf "" > system_sensitivity_result.txt
    # Epoch length
    printf "Epoch Length\n" >> system_sensitivity_result.txt
    setting="system_d2"
    SOURCE_TYPE="systemsensitivity"
    DELAY2=20
    DELAY3=500
    DELAY4=20
    DELAY5=1000
    STATE_SIZE2=20000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=20000
    STATE_SIZE4=20000
    STATE_SIZE5=20000
    LP2=1
    LP3=7
    LP4=1
    LP5=30

    P2=1
    P3=3
    P4=1
    P5=17
    GRAPH="1split2join1"
    autotuner_bar_lowerbound=450
    autotuner_latency_window=100
    autotuner_increase_bar_alpha=0.1
    epoch=100
    CURVE_TYPE="sine" #"linear"
    warmupRate=5000
    warmupTime=60
    RATE_I=5000
    TIME_I=0
    STAIRS=3
    amplitude_low=1000
    amplitude_high=3000
    period_low=75
    period_high=45

    for epoch in 25 50 100 200 500; do #
      is_treat=false
      autotune=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      for L in 2000; do # 1000
        is_treat=true
        autotune=true
        how_type="streamsluice"
#        run_one_exp
#        printf "${EXP_NAME}\n" >> system_sensitivity_result.txt
      done
    done

    # Resource sensitivity
    printf "Resource sensitivity\n" >> system_sensitivity_result.txt
    setting="system_d3"
    SOURCE_TYPE="systemsensitivity"
    DELAY2=20
    DELAY3=20
    DELAY4=20
    DELAY5=1000
    STATE_SIZE2=20000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=20000
    STATE_SIZE4=20000
    STATE_SIZE5=20000
    LP2=1
    LP3=1
    LP4=1
    LP5=36

    P2=1
    P3=1
    P4=1
    P5=17
    GRAPH="1split2join1"
    autotuner_bar_lowerbound=350
    autotuner_latency_window=100
    autotuner_increase_bar_alpha=0.1
    autotune_interval=60
    epoch=100
    CURVE_TYPE="sine" #"linear"
    warmupRate=5000
    warmupTime=60
    RATE_I=5000
    TIME_I=0
    STAIRS=3
    amplitude_low=1000
    amplitude_high=3000
    period_low=60
    period_high=30
    for autotuner_increase_bar_alpha in 0.0 0.5 1.0; do # 0.5 1.0
      is_treat=false
      autotune=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      for L in 2000; do #
        is_treat=true
        autotune=true
        how_type="streamsluice"
        run_one_exp
        printf "${EXP_NAME}\n" >> system_sensitivity_result.txt
      done
    done

    # Tuning window length
    printf "Tuning window Length\n" >> system_sensitivity_result.txt
    setting="system_d4"
    SOURCE_TYPE="systemsensitivity"
    DELAY2=20
    DELAY3=500
    DELAY4=20
    DELAY5=1000
    STATE_SIZE2=20000 # 1000 keys, per key (n * 2000 + 36) bytes, n=5000 -> 100 MB
    STATE_SIZE3=20000
    STATE_SIZE4=20000
    STATE_SIZE5=20000
    LP2=1
    LP3=7
    LP4=1
    LP5=30

    P2=1
    P3=3
    P4=1
    P5=17
    GRAPH="1split2join1"
    autotuner_bar_lowerbound=350
    autotuner_latency_window=100
    autotuner_increase_bar_alpha=0.5
    epoch=100
    CURVE_TYPE="sine" #"linear"
    warmupRate=5000
    warmupTime=60
    RATE_I=5000
    TIME_I=0
    STAIRS=3
    amplitude_low=1000
    amplitude_high=3000
    period_low=75
    period_high=45
    for autotune_interval in 15 60 240; do # 60 240
      is_treat=false
      autotune=false
      how_type="ds2"
#      run_one_exp
#      printf "${EXP_NAME}\n" >> workload_sensitivity_result.txt
      for L in 2000; do # 750 1250
        is_treat=true
        autotune=true
        how_type="streamsluice"
        run_one_exp
        printf "${EXP_NAME}\n" >> system_sensitivity_result.txt
      done
    done

}

run_scale_test

