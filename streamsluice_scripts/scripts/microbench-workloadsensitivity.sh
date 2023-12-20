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
  EXP_NAME=microbench-workload-${GRAPH_TYPE}-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${RANGE_I}-${PERIOD_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}-${L}-${migration_overhead}-${epoch}-${is_treat}-${repeat}

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
  graph_type=2op
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

  P2=3
  MP2=128
  DELAY2=200
  IO2=1
  STATE_SIZE2=100

  P3=6
  MP3=128
  DELAY3=500
  IO3=1
  STATE_SIZE3=100

  P4=4
  MP4=128
  DELAY4=333
  IO4=1
  STATE_SIZE4=100

  spike_estimation="linear_regression"
  spike_slope=0.7
  spike_intercept=500

  repeat=1
  warmup=10000
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -GRAPH_TYPE ${graph_type} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interRange ${RANGE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interRange ${RANGE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} &
}

run_scale_test(){
    echo "Run micro bench workload sensitivity..."
    init
    #L=1000
    #is_treat=false
    #repeat=1
    #run_one_exp

    PERIOD_I=120
    RATE1=10000
    RATE2=10000
    RATE_I=10000
    for RATE1 in 5000 6000 8000 7000 9000; do
        RATE2=${RATE1}
        L=1000
        is_treat=false
        repeat=1
        run_one_exp
          for is_treat in true; do
              for repeat in 1; do
                  run_one_exp
              done
          done
      done

    # Period
#    true_spike=400
#    RATE1=8000
#    RATE2=8000
#    for PERIOD_I in 30 48 60 80; do
#        L=1000
#        is_treat=false
#        repeat=1
#        run_one_exp
#        for L in 750 1000 1250 1500 2000; do
#            migration_overhead="$((${L}-${true_spike}))"
#            #migration_overhead="$(((${L}-${true_spike})/2+${true_spike}))"
#            migration_interval="$((${L}-${true_spike}))"
#            for is_treat in true; do
#                for repeat in 1; do
#                    run_one_exp
#                done
#            done
#        done
#    done

    # SKEW
#    PERIOD_I=120
#    for ZIPF_SKEW in 0.25 0.5 1; do
#        L=1000
#        is_treat=false
#        repeat=1
#        run_one_exp
#        for L in 750 1000 1250 1500 2000; do
#            migration_overhead="$((${L}-${true_spike}))"
#            #migration_overhead="$(((${L}-${true_spike})/2+${true_spike}))"
#            migration_interval="$((${L}-${true_spike}))"
#            for is_treat in true; do
#                for repeat in 1; do
#                    run_one_exp
#                done
#            done
#        done
#    done

    # State size
    #RATE1=8000
    #RATE2=8000
    #PERIOD_I=120
    #ZIPF_SKEW=0
    #for STATE_SIZE2 in 500 1000 2000 4000; do
    #    STATE_SIZE3=${STATE_SIZE2}
    #    STATE_SIZE4=${STATE_SIZE2}
    #    STATE_SIZE5=${STATE_SIZE2}
    #    is_treat=false
    #    repeat=1
    #    run_one_exp
    #    for L in 750 1000 1250 1500 2000; do
    #        for migration_overhead in 250 500 750 1000 1500; do
    #            if [[ ${L} -gt ${migration_overhead} ]]; then
    #                migration_interval="$((${L}-${STATE_SIZE2}))"
    #                for is_treat in true; do
    #                    for repeat in 1; do
    #                        run_one_exp
    #                    done
    #                done
    #            fi
    #        done
    #    done
    #done
}

run_scale_test

