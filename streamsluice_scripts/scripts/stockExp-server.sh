#!/bin/bash

source config-server.sh

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
  EXP_NAME=stock-server-${topology}-${stock_file_name}-${whether_type}-${how_type}-${runtime}-${warmup_time}-${warmup_rate}-${skip_interval}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${L}-${epoch}-${is_treat}-${repeat}

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
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5"
  L=2000
  migration_interval=500
  epoch=100
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StreamSluiceTestSet.StockTest"
  # only used in script
  runtime=90
  # set in Flink app
  stock_path="/home/samza/SSE_data/"
  stock_file_name="sb-4hr-50ms.txt"
  P1=1
  LP2=4
  P2=2 #3
  MP2=128
  DELAY2=1000
  IO2=1
  STATE_SIZE2=100

  LP3=9
  P3=3 #5
  MP3=128
  DELAY3=2000
  IO3=1
  STATE_SIZE3=100

  LP4=18
  P4=6 #12
  MP4=128
  DELAY4=5000
  IO4=1
  STATE_SIZE4=100

  LP5=15
  P5=4 #3
  MP5=128
  DELAY5=3333
  STATE_SIZE5=100
  WINDOW5=1

  repeat=1
  warmup=10000
  spike_estimation="linear_regression"
  spike_slope=0.7
  spike_intercept=1000
  is_treat=true
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5window ${WINDOW5} \
    -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} -topology ${topology} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5window ${WINDOW5} \
    -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} -topology ${topology} &
}

run_stock_test(){
    echo "Run overall test..."
    init
    L=1000
    runtime=3690
    warmup_rate=1000
    warmup_time=30
    skip_interval=20
    repeat=1
    STATE_SIZE2=500
    STATE_SIZE3=500
    STATE_SIZE4=500
    STATE_SIZE5=500
    spike_slope=0.8
    spike_intercept=150
    #topology="3op"
    topology="split3" #"split_join"
    if [[ "${topology}" == "split_join" || "${topology}" == "split3"  ]]; then
       vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
    fi

    printf "" > stock_result.txt
    run_one_exp
    printf "${EXP_NAME}\n" >> stock_result.txt


    is_treat=false
    run_one_exp
    printf "${EXP_NAME}\n" >> stock_result.txt
    is_treat=false
    P2=4
    P3=6
    P4=12
    P5=9
    run_one_exp
    printf "${EXP_NAME}\n" >> stock_result.txt
    is_treat=true

    P2=2
    P3=3
    P4=6
    P5=4
    whether_type="ds2"
    how_type="ds2"
    scalein_type="ds2"
    migration_interval=5000
    run_one_exp
    printf "${EXP_NAME}\n" >> stock_result.txt

    whether_type="streamswitch"
    how_type="streamswitch"
    scalein_type="streamswitch"
    migration_interval=1000
    run_one_exp
    printf "${EXP_NAME}\n" >> stock_result.txt
}
run_stock_test