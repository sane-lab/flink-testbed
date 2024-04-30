#!/bin/bash

source config-server-twitter.sh

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
  EXP_NAME=tweet_alert-${whether_type}-${how_type}-${runtime}-${warmup_time}-${warmup_rate}-${skip_interval}-${P2}-${DELAY2}-${P3}-${DELAY3}-${P4}-${DELAY4}-${P5}-${DELAY5}-${L}-${epoch}-${is_treat}-${errorcase_number}-${calibrate_selectivity}-${repeat}

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
  L=4000
  runtime=3990 #
  skip_interval=1 # skip seconds
  warmup=10000
  warmup_time=30
  warmup_rate=1500
  repeat=1
  spike_estimation="linear_regression"
  spike_slope=0.75
  spike_intercept=2000
  errorcase_number=3
  #calibrate_selectivity=false
  calibrate_selectivity=true
  vertex_id="feccfb8648621345be01b71938abfb72,a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47" #,36fcfcb61a35d065e60ee34fccb0541a" #,c395b989724fa728d0a2640c6ccdb8a1"
  is_treat=true
  migration_interval=500
  epoch=100
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.tweetalert.TweetAlertTrigger"
  # set in Flink app
  stock_path="/home/samza/Tweet_data/"
  stock_file_name="3hr-50ms.txt"
  MP1=1
  MP2=128
  MP3=128
  MP4=128
  MP5=128
  MP6=128
  MP7=128

  LP2=45
  LP3=25
  LP4=1
  LP5=30
  #LP6=1

  P1=1
  P2=45
  P3=25
  P4=1
  P5=30
  #P6=1

  DELAY2=5000
  DELAY3=3333
  DELAY4=100
  DELAY5=4000
  #DELAY6=100
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} \
    -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} \
    -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
        -p1 ${P1} -mp1 ${MP1} \
        -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} \
        -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} \
        -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} \
        -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} \
        -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} &
}

run_stock_test(){
    echo "Run twitter alert experiments..."
    init
    printf "" > tweet_result.txt

    for repeat in 1; do #2 3 4 5
        run_one_exp
        printf "${EXP_NAME}\n" >> tweet_result.txt

#        is_treat=false
#        run_one_exp
#        printf "${EXP_NAME}\n" >> tweet_result.txt
#        P1=2
#        P2=3
#        P3=9
#        P4=5
#        P5=6
#        P6=2
#        P7=8
#        is_treat=false
#        run_one_exp
#        printf "${EXP_NAME}\n" >> tweet_result.txt
#        is_treat=true

#        P1=1
#        P2=2
#        P3=6
#        P4=3
#        P5=4
#        P6=1
#        P7=5
#        whether_type="ds2"
#        how_type="ds2"
#        scalein_type="ds2"
#        migration_interval=2500
#        run_one_exp
#        printf "${EXP_NAME}\n" >> tweet_result.txt
#
#        whether_type="streamswitch"
#        how_type="streamswitch"
#        scalein_type="streamswitch"
#        migration_interval=1000
#        run_one_exp
#        printf "${EXP_NAME}\n" >> tweet_result.txt
    done
}
run_stock_test