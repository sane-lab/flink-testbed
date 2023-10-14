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
  job="flinkapp.StreamSluiceTestSet.DAGTest"
  # only used in script
  runtime=150
  # set in Flink app
  RATE1=8000
  TIME1=30
  RATE2=8000
  TIME2=40
  RATE_I=10000
  TIME_I=120
  PERIOD_I=60
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
run_workloads(){
    init
    RATE1=8000
    RATE2=8000
#    for PERIOD_I in 60 30 20 15; do
#        run_auto_tuner
#    done


#    PERIOD_I=60
#    for RATE1 in 5000 6000 7000 8000 9000; do
#        RATE2=$RATE1
#        run_auto_tuner
#    done
#
    RATE1=8000
    RATE2=$RATE1
    PERIOD_I=30
    for STATE_SIZE2 in 1 1000 2000 4000 8000; do
        STATE_SIZE3=${STATE_SIZE2}
        STATE_SIZE4=${STATE_SIZE2}
        STATE_SIZE5=${STATE_SIZE2}
        run_auto_tuner
    done
#
#    STATE_SIZE2=500
#    for ZIPF_SKEW in 0.1 0.2 0.4 0.8; do
#        P2=4
#        P3=10
#        P4=6
#        P5=6
#        run_auto_tuner
#    done

}

run_auto_tuner(){
    WORKLOAD_NAME=DAG-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${PERIOD_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}
    echo "Run auto tuner for workload ${WORKLOAD_NAME}"
    printf "\n\nTune log for workload ${WORKLOAD_NAME}\n" >> tune_log
    startTime=${TIME1}
    endTime="$((${TIME1}+${TIME_I}))"
    target_percentage=0.99
    true_spike="$((350+(${STATE_SIZE2}/2)))"
    L_L="$((${true_spike}+50))"
    L_R=10000
    i=8
    while [ $i -ge 1 ]; do
        L="$(((${L_L}+${L_R})/2))"
        i="$((${i}-1))"
        echo "Auto tuning round ${i}: top ${L_L} bottom ${L_R} current ${L}"
        printf "Auto tuning round ${i}: top ${L_L} bottom ${L_R} current ${L}\n" >> tune_log
        reachable=false
        for migration_overhead in "$((${L}-${true_spike}))" "$((${L}-${true_spike}-((${L}-${true_spike})/5)))" "$((${L}-${true_spike}-((${L}-${true_spike})/5*2)))" "$((${L}-${true_spike}-((${L}-${true_spike})/5*3)))" "$((${L}-${true_spike}-((${L}-${true_spike})/5*4)))"; do
            migration_interval=$migration_overhead
            run_one_exp
            python checkLatencyLimit.py ${EXP_DIR}/raw/${EXP_NAME} ${L} ${target_percentage} ${startTime} ${endTime} || printf "0\n0.0\n" > "reachOrNot.txt"
            index=0
            while IFS= read -r line; do
                #echo "Text read from file: ${line}"
                if [[ ${index} == 0 ]]; then
                  successResult=$line
                else
                  successRate=$line
                fi
                index="$((${index}+1))"
            done < "reachOrNot.txt"
            printf "${L} ${migration_overhead} ${successRate} ${reachable}\n" >> tune_log
            if [[ ${successResult} == "1" ]]; then
                reachable=true
                break
            fi
        done
        printf "${L} ${reachable}\n" >> tune_log
        if [[ ${reachable} == "true" ]]; then
            L_R=${L}
        else
            L_L=${L}
        fi
    done
    printf "${L_R}\n" >> tune_log
    printf "${WORKLOAD_NAME} ${L_R}\n" >> tune_result
}

#printf "" > tune_result
#printf "" > tune_log

run_workloads

