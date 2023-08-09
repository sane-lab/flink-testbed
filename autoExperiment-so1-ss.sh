#!/bin/bash

#FLINK_DIR="/home/samza/workspace/flink-related/flink-extended-ete/build-target"
FLINK_DIR="/home/samza/workspace/flink-related/flink-extended-ete/build-target"
FLINK_APP_DIR="/home/samza/workspace/flink-related/flink-testbed-sane"

# clean kafka related data
function cleanEnv() {
    rm -rf /tmp/flink*
    export JAVA_HOME=/home/samza/kit/jdk
    ~/samza-hello-samza/bin/grid stop kafka
    ~/samza-hello-samza/bin/grid stop zookeeper
    kill -9 $(jps | grep Kafka | awk '{print $1}')
    rm -r /data/kafka/kafka-logs/
    rm -r /tmp/kafka-logs/
    rm -r /tmp/zookeeper/

    python -c 'import time; time.sleep(20)'

    ~/samza-hello-samza/bin/grid start zookeeper
    ~/samza-hello-samza/bin/grid start kafka

    python -c 'import time; time.sleep(5)'
}

# configure parameters in flink bin
function configFlink() {
    # set user requirement
    sed 's/^\(\s*streamswitch.requirement.latency\s*:\s*\).*/\1'"$L"'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
    sed 's/^\(\s*streamsluice.system.migration_overhead\s*:\s*\).*/\1'"$migration_overhead"'/' tmp > tmp2 
    sed 's/^\(\s*model.vertex\s*:\s*\).*/\1'"$vertex_id"'/' tmp2 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp tmp2

    # set static or streamswitch
    if [[ ${isTreat} == 1 ]]
    then
        sed 's/^\(\s*streamswitch.system.is_treat\s*:\s*\).*/\1'true'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
        mv tmp ${FLINK_DIR}/conf/flink-conf.yaml
    elif [[ ${isTreat} == 0 ]]
    then
        sed 's/^\(\s*streamswitch.system.is_treat\s*:\s*\).*/\1'false'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
        mv tmp ${FLINK_DIR}/conf/flink-conf.yaml
    fi

    # set scale-in
    if [[ ${isScaleIn} == 1 ]]
    then
        sed 's/^\(\s*streamswitch.system.is_scalein\s*:\s*\).*/\1'true'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
        mv tmp ${FLINK_DIR}/conf/flink-conf.yaml
    elif [[ ${isScaleIn} == 0 ]]
    then
        sed 's/^\(\s*streamswitch.system.is_scalein\s*:\s*\).*/\1'false'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
        mv tmp ${FLINK_DIR}/conf/flink-conf.yaml
    fi
}

# run flink clsuter
function runFlink() {
    if [[ -d ${FLINK_DIR}/log ]]; then
        rm -rf ${FLINK_DIR}/log
    fi
    mkdir ${FLINK_DIR}/log
    ${FLINK_DIR}/bin/start-cluster.sh
}

# run applications
function runApp() {
    echo "run Multi"
    ${FLINK_APP_DIR}/submit-so1-ss.sh ${N1} ${MP1} ${TIME1} ${RATE1} ${TIME2} ${RATE2} ${TIME_I} ${RATE_I} ${PERIOD_I} ${INTERVAL} 1
}

# clsoe flink clsuter
function closeFlink() {
    echo "experiment finished closing it"
    ${FLINK_DIR}/bin/stop-cluster.sh
    if [[ -d ${FLINK_APP_DIR}/nexmark_scripts/draw/logs/${EXP_NAME} ]]; then
        rm -rf ${FLINK_APP_DIR}/nexmark_scripts/draw/logs/${EXP_NAME}
    fi
    mv ${FLINK_DIR}/log ${FLINK_APP_DIR}/multi_result/${EXP_NAME}
    echo "close finished"
}

# draw figures
function draw() {
    #python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/RateAndWindowDelay.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/ViolationsAndUsageFromGroundTruth.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
}

# set in Flink
L=2000
migration_overhead=1000
isTreat=1
isScaleIn=1
vertex_id="22359d48bcb33236cf1e31888091e54c"


# only used in script
SUMRUNTIME=300 #240
# set in Flink app
RATE1=400
TIME1=60
RATE2=600
TIME2=60
RATE_I=500
TIME_I=120
PERIOD_I=240
N1=5
MP1=64
#RATE=100000
INTERVAL=50
repeat=1
TOTAL=-1
for RATE1 in 400; do # 6000 50000 100000
#for RATE in 50000 100000 150000; do # 0 5000 10000 15000 20000 25000 30000
  for CYCLE in 120; do # 60 90
        #for repeat in 1 2 3; do # only used for repeat exps, no other usage
        WARMRATE=$RATE
        RUNTIME=`expr ${SUMRUNTIME} - ${WARMUP} - 10`
        EXP_NAME=SO-R1${RATE1}R2${RATE2}RI${RATE_I}TI${TIME_I}PI${PERIOD_I}-N1_${N1}-L${L}mo${migration_overhead}-T${isTreat}-R${RUNTIME}-${repeat}
        echo ${EXP_NAME}

        #cleanEnv
        #configFlink
        #runFlink
        runApp

        python -c 'import time; time.sleep('"${SUMRUNTIME}"')'

        # draw figure
        #mkdir ./multi_result/${EXP_NAME}
        #draw
        #closeFlink
        ~/samza-hello-samza/deploy/kafka/bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic flink_metrics --from-beginning > ./nexmarkQ5_result/${EXP_NAME}/metrics &
        python -c 'import time; time.sleep(30)'
        kill -9 $(jps | grep ConsoleConsumer | awk '{print $1}')
    done
done
