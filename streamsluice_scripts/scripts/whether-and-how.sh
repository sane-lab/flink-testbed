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
  EXP_NAME=whetherhow-${whether_type}-${how_type}-${is_treat}-${runtime}-${CURVE_TYPE}-${RATE1}-${RATE2}-${RATE_I}-${PERIOD_I}-${P1}-${ZIPF_SKEW}-${P2}-${DELAY2}-${IO2}-${STATE_SIZE2}-${P3}-${DELAY3}-${IO3}-${STATE_SIZE3}-${P4}-${DELAY4}-${IO4}-${STATE_SIZE4}-${P5}-${DELAY5}-${STATE_SIZE5}-${L}-${migration_overhead}-${epoch}-${repeat}

  echo "INFO: run exp ${EXP_NAME}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

  analyze
  stopFlink

  python -c 'import time; time.sleep(1)'
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
  CURVE_TYPE="sine"
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StreamSluiceTestSet.FourOperatorTest"
  # only used in script
  runtime=50
  # set in Flink app
  RATE1=10000
  TIME1=30
  RATE2=10000
  TIME2=120
  RATE_I=12000
  TIME_I=60
  PERIOD_I=30
  ZIPF_SKEW=0
  NKEYS=1000
  P1=1

  P2=1
  MP2=128
  DELAY2=125
  IO2=1
  STATE_SIZE2=1000

  P3=2
  MP3=128
  DELAY3=250
  IO3=1
  STATE_SIZE3=1000

  P4=3
  MP4=128
  DELAY4=444 #500
  IO4=1
  STATE_SIZE4=1000

  P5=5
  MP5=128
  DELAY5=1000
  STATE_SIZE5=1000


  repeat=1
  warmup=10000
  is_treat=true
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} \
    -curve_type ${CURVE_TYPE}  &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} -op2IoRate ${IO2} -op2KeyStateSize ${STATE_SIZE2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} -op3IoRate ${IO3} -op3KeyStateSize ${STATE_SIZE3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} -op4IoRate ${IO4} -op4KeyStateSize ${STATE_SIZE4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} -op5KeyStateSize ${STATE_SIZE5} \
    -nkeys ${NKEYS} -phase1Time ${TIME1} -phase1Rate ${RATE1} -phase2Time ${TIME2} \
    -phase2Rate ${RATE2} -interTime ${TIME_I} -interRate ${RATE_I} -interPeriod ${PERIOD_I} -zipf_skew ${ZIPF_SKEW} \
    -curve_type ${CURVE_TYPE}  &
}

run_scale_test(){
    echo "Run whether-how test..."
    init
    L=2000
    migration_overhead=950
    #true_spike="$((350+(${STATE_SIZE2}/2)))"
    migration_interval=2000
    # Whether 1
    RATE1=4000
    RATE2=6000
    RATE_I=5000
    PERIOD_I=20
    TIME_I=10
    printf "" > whetherhow_result.txt
    for CURVE_TYPE in "gradient"; do # "linear"; do #"sine" "gradient"; do #; do
      is_treat=false
      #run_one_exp
      #printf "1_${CURVE_TYPE} ${EXP_NAME}\n" >> whetherhow_result.txt
      is_treat=true
      whether_type="streamsluice"
      how_type="streamsluice"
      #run_one_exp
      #printf "1_${CURVE_TYPE} ${EXP_NAME}\n" >> whetherhow_result.txt

      if [[ ${CURVE_TYPE} == "sine" ]]; then
          # time 22.7 scale-out 5->8
          whether_early="time_21"
          whether_late="time_25"
      elif [[ ${CURVE_TYPE} == "linear" ]]; then
          # time 23.3 5->8
          whether_early="time_21"
          whether_late="time_25"
      elif [[ ${CURVE_TYPE} == "gradient" ]]; then
          # time 15.9 5->8
          whether_early="time_14"
          whether_late="time_18"
      fi
      for whether_type in ${whether_early} ${whether_late}; do
        how_type="streamsluice"
        #run_one_exp
        #printf "1_${CURVE_TYPE} ${EXP_NAME}\n" >> whetherhow_result.txt
      done
    done

    # Whether 2 & 3
    RATE1=4000
    RATE2=4000
    RATE_I=5000
    PERIOD_I=10
    TIME_I=10
    migration_overhead=200 #200
    for CURVE_TYPE in "sine"; do # "linear"; do #"sine" "gradient"; do #; do
      #is_treat=false
      #run_one_exp
      #printf "2_${CURVE_TYPE} ${EXP_NAME}\n" >> whetherhow_result.txt
      #is_treat=true
      #whether_type="streamsluice"
      #how_type="streamsluice"
      #run_one_exp
      #printf "2_${CURVE_TYPE} ${EXP_NAME}\n" >> whetherhow_result.txt

      if [[ ${CURVE_TYPE} == "sine" ]]; then
          #  5->8
          whether_early="time_16"
          whether_late=""
      elif [[ ${CURVE_TYPE} == "linear" ]]; then
          #
          whether_early="time_21"
          whether_late="time_25"
      elif [[ ${CURVE_TYPE} == "gradient" ]]; then
          # 16.4
          whether_early="time_14"
          whether_late="time_18"
      fi
      for whether_type in ${whether_early} ${whether_late}; do
        how_type="streamsluice"
        #run_one_exp
        #printf "2_${CURVE_TYPE} ${EXP_NAME}\n" >> whetherhow_result.txt
      done
    done

    # How
    L=2000
    migration_overhead=950
    #true_spike="$((350+(${STATE_SIZE2}/2)))"
    migration_interval=2000
    RATE1=4000
    RATE2=6000
    RATE_I=5000
    PERIOD_I=20
    TIME_I=10
#    STATE_SIZE2=4000
#    STATE_SIZE3=4000
#    STATE_SIZE4=4000
#    STATE_SIZE5=4000
    is_treat=true
    for CURVE_TYPE in "sine" "gradient"; do # "linear"; do #"sine" "gradient"; do #; do
      is_treat=false
      run_one_exp
      printf "${EXP_NAME}\n" >> whetherhow_result.txt
      is_treat=true
      whether_type="streamsluice"
      how_type="streamsluice"
      run_one_exp
      printf "${EXP_NAME}\n" >> whetherhow_result.txt

#      if [[ ${CURVE_TYPE} == "sine" ]]; then
#          # time 22.7 scale-out 5->8
#          whether_type="time_23"
#      fi
      for how_type in "op_2_6_keep" "op_2_1_keep" "op_1_2_keep"; do
        run_one_exp
        printf "${EXP_NAME}\n" >> whetherhow_result.txt
      done
    done


    # How 2
    L=2000
    migration_overhead=900
    #true_spike="$((350+(${STATE_SIZE2}/2)))"
    migration_interval=2000
    RATE1=4000
    RATE2=4000
    RATE_I=5000
    PERIOD_I=10
    TIME_I=10
#    STATE_SIZE2=4000
#    STATE_SIZE3=4000
#    STATE_SIZE4=4000
#    STATE_SIZE5=4000
    is_treat=true

    for CURVE_TYPE in "gradient"; do # "linear"; do #"sine" "gradient"; do #; do
      is_treat=false
      run_one_exp
      printf "${CURVE_TYPE} ${EXP_NAME}\n" >> whetherhow_result.txt
      is_treat=true
      whether_type="streamsluice"
      how_type="streamsluice"
      run_one_exp
      printf "${CURVE_TYPE} ${EXP_NAME}\n" >> whetherhow_result.txt

#      if [[ ${CURVE_TYPE} == "sine" ]]; then
#          # time 22.7 scale-out 5->8
#          whether_type="time_23"
#      fi
      for how_type in "op_2_6_keep" "op_2_1_keep" "op_1_2_keep"; do
        run_one_exp
        printf "${EXP_NAME}\n" >> whetherhow_result.txt
      done
    done



#    is_treat=false
#    repeat=1
#    run_one_exp
#    printf "${EXP_NAME}\n" >> finished_exps
#    is_treat=true
#    for whether_type in "streamsluice"; do
#      for how_type in "streamsluice"; do
#        run_one_exp
#        printf "${EXP_NAME}\n" >> finished_exps
#      done
#    done
#    printf "" > part3curve3
#    is_treat=true
#    for whether_type in "time_21" "time_31"; do #"streamsluice"; do
#      for how_type in "streamsluice" "op_2_1_keep"; do
#        run_one_exp
#        printf "${EXP_NAME}\n" >> part3curve3
#      done
#    done
#    for whether_type in "time_26" "streamsluice"; do
#      for how_type in "op_2_0_shuffle" "op_2_3_keep" "op_2_2_shuffle" "op_1_2_keep"; do
#        run_one_exp
#        printf "${EXP_NAME}\n" >> part3curve3
#      done
#    done


}

run_scale_test

