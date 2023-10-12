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
  EXP_NAME=${WORKLOAD_NAME}-autotune-${L}-${migration_overhead}

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
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
  L=1000
  migration_overhead=500
  migration_interval=500
  epoch=100
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StreamSluiceTestSet.FourOperatorTest"
  # only used in script
  runtime=300
  # set in Flink app
  RATE1=8000
  TIME1=30
  RATE2=8000
  TIME2=40
  RATE_I=10000
  TIME_I=240
  PERIOD_I=120
  ZIPF_SKEW=0
  NKEYS=1000
  P1=1

  P2=2
  MP2=128
  DELAY2=200
  IO2=1
  STATE_SIZE2=100

  P3=5
  MP3=128
  DELAY3=500
  IO3=1
  STATE_SIZE3=100

  P4=3
  MP4=128
  DELAY4=333
  IO4=1
  STATE_SIZE4=100

  P5=3
  MP5=128
  DELAY5=250
  STATE_SIZE5=100


  repeat=1
  warmup=10000
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} &
}

run_auto_tuner(){
    echo "Run auto tuner..."
    init
    PERIOD_I=60
    WORKLOAD_NAME=4op-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${PERIOD_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}
    L_L=500
    L_R=10000
    target_percentage=99.9
    i=10
    while [ $i -ge 1 ]; do
        L="$(((${L_L}+${L_R})/2))"
        i="$((${i}-1))"
        echo "Auto tuning round ${i}: top ${L_L} bottom ${L_R} current ${L}"
        reachable=false
        for migration_overhead in "$(((${L}/5)))" "$(((${L}/5)*2))" "$(((${L}/5)*3))" "$(((${L}/5)*4))"; do
            migration_interval=$migration_overhead
            run_one_exp
            python checkLatencyLimit.py ${EXP_DIR}/raw/${EXP_NAME} ${L} ${target_percentage}
            while IFS= read -r line; do
                echo "Text read from file: ${line}"
                successResult=$line
            done < reachOrNot.txt
            printf "${L} ${migration_overhead} ${reachable}\n" >> tune_result
            if [[ ${successResult} == "1" ]]; then
                reachable=true
            fi
        done
        printf "${L} ${reachable}\n" >> tune_result
        if [[ ${reachable} == "true" ]]; then
            L_R=${L}
        else
            L_L=${L}
        fi
    done
    printf "${L_R}\n" >> tune_result
}

run_auto_tuner