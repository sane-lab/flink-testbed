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
    ${FLINK_APP_DIR}/submit-multi-ss.sh ${N1} ${MP1} ${N2} ${MP1} ${RUNTIME} ${RATE} ${CYCLE} ${AMPL} ${WARMUP} ${WARMRATE} ${INTERVAL} ${TOTAL} 1
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
vertex_id="c21234bcbf1e8eb4c61f1927190efebd,22359d48bcb33236cf1e31888091e54c"


# only used in script
SUMRUNTIME=360 #240
# set in Flink app
RATE=0
CYCLE=120
N1=8
MP1=30
N2=15
#RATE=100000
WARMUP=60
INTERVAL=50
repeat=1
TOTAL=-1
for RATE in 3000; do # 6000 50000 100000
#for RATE in 50000 100000 150000; do # 0 5000 10000 15000 20000 25000 30000
    for AMPL in 2000; do # 60 75 90 105 120
    for CYCLE in 120; do # 60 90
        #for repeat in 1 2 3; do # only used for repeat exps, no other usage
            WARMRATE=$RATE
            RUNTIME=`expr ${SUMRUNTIME} - ${WARMUP} - 10`
            EXP_NAME=Multi-R${RATE}C${CYCLE}A${AMPL}-N1_${N1}-N2_${N2}-L${L}mo${migration_overhead}-T${isTreat}-R${RUNTIME}-${repeat}
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
done
