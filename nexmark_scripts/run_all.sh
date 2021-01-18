#!/bin/bash

FLINK_DIR="/home/myc/workspace/flink-related/flink/build-target"
FLINK_APP_DIR="/home/myc/workspace/flink-related/flink-testbed-org"

# run flink clsuter
function runFlink() {
    echo "starting the cluster"
    if [[ -d ${FLINK_DIR}/log ]]; then
        rm -rf ${FLINK_DIR}/log
    fi
    mkdir ${FLINK_DIR}/log
    ${FLINK_DIR}/bin/start-cluster.sh
}

# clsoe flink clsuter
function stopFlink() {
    echo "experiment finished, stopping the cluster"
    PID=`jps | grep CliFrontend | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
          ${FLINK_DIR}/bin/stop-cluster.sh
    fi
    echo "close finished"
}

# configure parameters in flink bin
function configFlink() {
    # set user requirement
    sed 's/^\(\s*trisk.reconfig.operator.name\s*:\s*\).*/\1'"$operator"'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp1
    sed 's/^\(\s*trisk.reconfig.frequency\s*:\s*\).*/\1'"$frequency"'/' tmp1 > tmp2
    sed 's/^\(\s*trisk.reconfig.affected_task\s*:\s*\).*/\1'"$affected_tasks"'/' tmp2 > tmp3
    sed 's/^\(\s*trisk.reconfig.type\s*:\s*\).*/\1'"$type"'/' tmp3 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp1 tmp2 tmp3
}

# clean kafka related data
function cleanEnv() {
    rm -rf /tmp/flink*
#    rm ${FLINK_DIR}/log/*
}


# run applications
function runApp() {
  echo "run app in ${JAR}"
  ${FLINK_DIR}/bin/flink run -c flinkapp.StatefulDemoLongRun ${JAR} -runtime ${runtime} -nTuples ${n_tuples} -p2 ${parallelism} &
}


# draw figures
function analyze() {
    #python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/RateAndWindowDelay.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    echo "dump to /data/trisk-${type}-${frequency}"
    cp -r /data/trisk /data/trisk-${type}-${frequency}
}

run_one_exp() {
  configFlink
  runFlink
  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

  analyze

  stopFlink
}

run_all() {
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  n_tuples=10000000
  runtime=100
  parallelism=10

  # system level
  operator="Splitter FlatMap"
  frequency=5
  affected_tasks=2
  type="noop"

  for frequency in 5; do # 0 1 5 10 100
    for n_tuples in 1000000 10000000 100000000; do
      for type in "noop" "remap"; do # "noop" "remap" "rescale"
        if [[ $type == "remap" ]]; then
          for affected_tasks in 2 4; do # 2 4 6 8 10
            run_one_exp
          done
        else
          run_one_exp
        fi
      done
    done
  done
}


run_all

# dump the statistics when all exp are finished
# in the future, we will draw the intuitive figures
python ./analysis/PerformanceAnalyzer.py