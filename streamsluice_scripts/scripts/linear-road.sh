#!/bin/bash

source config-server-lr.sh

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

    for host in "dragon" "eagle" "flamingo" "giraffe"; do
      scp ${host}:${FLINK_DIR}/log/* ${EXP_DIR}/raw/${EXP_NAME}/
      ssh ${host} "rm ${FLINK_DIR}/log/*"
    done
}

run_one_exp() {
  EXP_NAME=linear_road-${whether_type}-${how_type}-${runtime}-${warmup_time}-${warmup_rate}-${skip_interval}-${P2}-${DELAY2}-${P3}-${DELAY3}-${P4}-${DELAY4}-${P5}-${DELAY5}-${L}-${epoch}-${input_rate_factor}-${is_treat}-${errorcase_number}-${calibrate_selectivity}-${repeat}

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
  scalein_type="streamsluice"
  L=2000
  runtime=2190 #1290 #3990
  skip_interval=10 #120 #300 # skip seconds
  warmup=10000
  warmup_time=30
  warmup_rate=1000
  repeat=1
  spike_estimation="linear_regression"
  spike_slope=0.75
  spike_intercept=1000
  errorcase_number=3
  #calibrate_selectivity=false
  calibrate_selectivity=true
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47,36fcfcb61a35d065e60ee34fccb0541a,c395b989724fa728d0a2640c6ccdb8a1,8e0d1d377d577c52511ad507bf0ce330,feccfb8648621345be01b71938abfb72" # ,
  is_treat=true
  migration_interval=500
  epoch=100
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.linearroad.LinearRoad"
  # set in Flink app
  stock_path="/home/samza/LR_data/"
  stock_file_name="3hr-our-rate.txt" #"3hr-50ms.txt"
  MP1=1
  MP2=128
  MP3=128
  MP4=128
  MP5=128
#  MP6=128
#  MP7=128
#  MP8=128
#  MP9=128

  LP2=2
  LP3=20
  LP4=5
  LP5=70
#  LP6=1
#  LP7=60
#  LP8=1
#  LP9=1

  P1=1
  P2=2
  P3=20
  P4=4
  P5=70
#  P6=1
#  P7=65
#  P8=1
#  P9=1

  DELAY2=100
  DELAY3=2000 #2000
  DELAY4=100
  DELAY5=1500 #1500
#  DELAY6=10
#  DELAY7=500
#  DELAY8=10
#  DELAY9=100
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} \
    -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} \
    -p6 ${P6} -mp6 ${MP6} -op6Delay ${DELAY6} \
    -p7 ${P7} -mp7 ${MP7} -op7Delay ${DELAY7} \
    -p8 ${P8} -mp8 ${MP8} -op8Delay ${DELAY8} \
    -p9 ${P9} -mp9 ${MP9} -op9Delay ${DELAY9} \
    -input_rate_factor ${input_rate_factor} \
    -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
        -p1 ${P1} -mp1 ${MP1} \
        -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} \
        -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} \
        -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} \
        -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} \
        -p6 ${P6} -mp6 ${MP6} -op6Delay ${DELAY6} \
        -p7 ${P7} -mp7 ${MP7} -op7Delay ${DELAY7} \
        -p8 ${P8} -mp8 ${MP8} -op8Delay ${DELAY8} \
        -p9 ${P9} -mp9 ${MP9} -op9Delay ${DELAY9} \
        -input_rate_factor ${input_rate_factor} \
        -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} &
}

run_stock_test(){
    echo "Run linear road experiments..."
    init
    printf "" > lr_result.txt

    for repeat in 1; do # 2 3 4 5; do
        whether_type="streamsluice"
        how_type="streamsluice"
        scalein_type="streamsluice"
#        run_one_exp
#        printf "${EXP_NAME}\n" >> lr_result.txt
#
##        is_treat=false
##        run_one_exp
##        printf "${EXP_NAME}\n" >> lr_result.txt
##        P1=2
##        P2=3
##        P3=9
##        P4=5
##        P5=6
##        P6=2
##        P7=8
##        is_treat=false
##        run_one_exp
##        printf "${EXP_NAME}\n" >> lr_result.txt
##        is_treat=true
#
##        P1=1
##        P2=2
##        P3=6
##        P4=3
##        P5=4
##        P6=1
##        P7=5
#        whether_type="ds2"
#        how_type="ds2"
#        scalein_type="ds2"
#        migration_interval=2500
#        run_one_exp
#        printf "${EXP_NAME}\n" >> lr_result.txt
#
#        whether_type="streamswitch"
#        how_type="streamswitch"
#        scalein_type="streamswitch"
#        migration_interval=1000
#        run_one_exp
#        printf "${EXP_NAME}\n" >> lr_result.txt
    done

#       Part 2 experiment
    migration_interval=500
    DELAY2=100
    DELAY3=2000 #2000
    DELAY4=100
    DELAY5=1500 #1500
    for input_rate_factor in 0.5 0.75 1.5; do
        run_one_exp
        printf "${EXP_NAME}\n" >> lr_result.txt
    done
    input_rate_factor=1
#    for process_factor in 2 3 5 6; do
#      DELAY3=$((${process_factor} * 500))
#      DELAY5=$(((${process_factor}-1) * 500))
#      run_one_exp
#      printf "${EXP_NAME}\n" >> lr_result.txt
#    done
    DELAY2=100
    DELAY3=2000
    DELAY4=100
    DELAY5=1500
}
run_stock_test