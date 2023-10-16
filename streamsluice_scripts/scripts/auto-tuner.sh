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

  python -c 'import time; time.sleep(2)'

  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

  analyze
  stopFlink

  python -c 'import time; time.sleep(2)'
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
  job="flinkapp.StreamSluiceTestSet.FourOperatorTest" # "flinkapp.StreamSluiceTestSet.DAGTest"
  # only used in script
  runtime=90
  # set in Flink app
  RATE1=10000
  TIME1=30
  RATE2=10000
  TIME2=20
  RATE_I=12000
  TIME_I=60
  PERIOD_I=30
  ZIPF_SKEW=0
  NKEYS=1000
  P1=1

#  P2=2
#  MP2=128
#  DELAY2=200
#  IO2=1
#  STATE_SIZE2=100
#
#  P3=5
#  MP3=128
#  DELAY3=500
#  IO3=1
#  STATE_SIZE3=100
#
#  P4=3
#  MP4=128
#  DELAY4=333
#  IO4=1
#  STATE_SIZE4=100
#
#  P5=3
#  MP5=128
#  DELAY5=250
#  STATE_SIZE5=100
  P2=2
  MP2=128
  DELAY2=125
  IO2=1
  STATE_SIZE2=1000

  P3=4
  MP3=128
  DELAY3=250
  IO3=1
  STATE_SIZE3=1000

  P4=6
  MP4=128
  DELAY4=500
  IO4=1
  STATE_SIZE4=1000

  P5=12
  MP5=128
  DELAY5=1000
  STATE_SIZE5=1000

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

    RATE1=10000
    RATE2=10000
    RATE_I=13000
#    for PERIOD_I in 60 40 30 20 15; do
#        run_auto_tuner
#    done

    RATE1=10000
    RATE2=10000
    PERIOD_I=30
#    for RATE_I in 15000 14000 12000 11000 ; do
#        run_auto_tuner
#    done
#
    RATE_I=13000
    PERIOD_I=30
#    for STATE_SIZE2 in 500 2000 3000 4000; do
#        STATE_SIZE3=${STATE_SIZE2}
#        STATE_SIZE4=${STATE_SIZE2}
#        STATE_SIZE5=${STATE_SIZE2}
#        run_auto_tuner
#    done




    STATE_SIZE2=1000
    STATE_SIZE3=${STATE_SIZE2}
    STATE_SIZE4=${STATE_SIZE2}
    STATE_SIZE5=${STATE_SIZE2}
    P2=4
    P3=6
    P4=12
    P6=16
    for ZIPF_SKEW in 0.05 0.1 0.2 0.4; do
        run_auto_tuner
    done
}

run_auto_tuner(){
    mkdir ${outputDir}
    WORKLOAD_NAME=4op-${runtime}-${RATE1}-${RATE2}-${RATE_I}-${PERIOD_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}
    outputFile="${outputDir}/${WORKLOAD_NAME}.out"
    echo "Run auto tuner for workload ${WORKLOAD_NAME}"
    printf "\n\nTune log for workload ${WORKLOAD_NAME}\n" > ${outputFile}
    startTime=${TIME1}
    endTime="$((${TIME1}+${TIME_I}))"
    target_percentage=0.99
    true_spike="$((350+(${STATE_SIZE2}/2)))"
    latency_lowerBound="400"
    L_L=1 #"$((${true_spike}+50))"
    L_R=4000
    i=8
    while [ $i -ge 1 ]; do
        L="$(((${L_L}+${L_R})/2))"
        i="$((${i}-1))"
        echo "Auto tuning round ${i}: top ${L_L} bottom ${L_R} current ${L}"
        printf "Auto tuning round ${i}: top ${L_L} bottom ${L_R} current ${L}\n" >> ${outputFile} #tune_log
        reachable=false
        #for migration_overhead in "$((${L}-${true_spike}))" "$((${L}-${true_spike}-((${L}-${true_spike})/5)))" "$((${L}-${true_spike}-((${L}-${true_spike})/5*2)))" "$((${L}-${true_spike}-((${L}-${true_spike})/5*3)))" "$((${L}-${true_spike}-((${L}-${true_spike})/5*4)))"; do
        for migration_overhead in "$((${L}-${latency_lowerBound}))" "$((${L}-${latency_lowerBound}-100))" "$((${L}-(${L}-${latency_lowerBound})/2))" "$((${L}-(${L}-${latency_lowerBound})/3))"; do
          if [[ ${migration_overhead} -ge 1 ]]; then
            migration_interval=${true_spike} #$migration_overhead
            run_one_exp
            python checkLatencyLimit.py ${EXP_DIR}/raw/${EXP_NAME} ${L} ${target_percentage} ${startTime} ${endTime} || printf "0\n0.0\n0\n0\n0\n0\n" > "reachOrNot.txt"
            index=0
            while IFS= read -r line; do
                #echo "Text read from file: ${line}"
                if [[ ${index} == 0 ]]; then
                  successResult=$line
                elif [[ ${index} == 1 ]]; then
                  successRate=$line
                elif [[ ${index} == 2 ]]; then
                  avgParallelism=$line
                elif [[ ${index} == 3 ]]; then
                  maxParallelism=$line
                elif [[ ${index} == 4 ]]; then
                  maxSpike=$line
                elif [[ ${index} == 5 ]]; then
                  avgSpike=$line
                fi
                index="$((${index}+1))"
            done < "reachOrNot.txt"
            printf "${L} ${migration_overhead} ${successRate} ${maxParallelism} ${avgParallelism} ${maxSpike} ${avgSpike} ${reachable}\n" >> ${outputFile} # tune_log
            if [[ ${successResult} == "1" ]]; then
                reachable=true
                break
            fi
          fi
        done
        printf "${L} ${reachable}\n" >> ${outputFile} # tune_log
        if [[ ${reachable} == "true" ]]; then
            L_R=${L}
        else
            L_L=${L}
        fi
    done
    # printf "${L_R}\n" >> ${outputFile} # tune_log
    printf "${WORKLOAD_NAME} best limit: ${L_R}\n" >> ${outputFile} # tune_result
}

#printf "" > tune_result
#printf "" > tune_log
outputDir="/data/streamsluice/autotuner"

run_workloads

