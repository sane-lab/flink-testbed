#!/bin/bash

source config-mlscore-server.sh

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

    for host in "dragon" "eagle"; do
      scp ${host}:${FLINK_DIR}/log/* ${EXP_DIR}/raw/${EXP_NAME}/
      ssh ${host} "rm ${FLINK_DIR}/log/*"
    done
}

run_one_exp() {
  EXP_NAME=ml-scoring-${setting}-${controller_type}-${whether_type}-${how_type}-${runtime}-${base_rate}-${sine_amplitude}-${sine_period}-${spike_probability}-${spike_multiplier}-${fluctuation_std}-${parse_delay}-${feature_delay}-${P1}-${P2}-${P3}-${P4}-${repeat}

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
  controller_type="StreamSluice"
  whether_type="streamsluice"
  how_type="streamsluice"
  vertex_id="parse_txn,feature_builder,scorer"
  L=1000
  migration_interval=500
  epoch=100
  decision_interval=100
  
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.MLmodelscoring.MLScoringJob"
  
  # ML scoring job specific parameters
  runtime=300
  base_rate=500
  sine_amplitude=0.3
  sine_period=60.0
  spike_probability=0.05
  spike_multiplier=3.0
  fluctuation_std=0.1
  parse_delay=1
  feature_delay=1
  
  # Feature-based scorer configuration parameters
  scorer_base_delay=2         # Base processing delay in ms
  scorer_complexity_factor=1.0 # Complexity multiplier for feature-based processing
  latency_output_file="/tmp/ml_scoring_latency.log"
  
  # parallelism settings
  P1=1
  MP1=1
  P2=1
  MP2=8
  P3=1
  MP3=8
  P4=1
  MP4=8
  
  # ML-specific max parallelism limits for vertex-based scaling
  LP_PARSE=8     # max parallelism for parse_txn operator
  LP_FEATURE=8   # max parallelism for feature_builder operator
  LP_SCORER=8    # max parallelism for scorer operator
  
  # ML-specific configuration parameters passed to Flink config
  ml_base_rate=${base_rate}
  ml_sine_amplitude=${sine_amplitude}
  ml_sine_period=${sine_period}
  ml_spike_probability=${spike_probability}
  ml_spike_multiplier=${spike_multiplier}
  ml_fluctuation_std=${fluctuation_std}
  ml_parse_delay=${parse_delay}
  ml_feature_delay=${feature_delay}
  
  # system settings
  is_treat=true
  repeat=1
  warmup=10000
  metrics_output=true
  autotune=true
  autotune_interval=1000
  autotuner_latency_window=5000
  autotuner_bar_lowerbound=300
  autotuner_initial_value_option=2
  autotuner_initial_value_alpha=0.2
  autotuner_adjustment_option=1
  autotuner_adjustment_beta=2.0
  autotuner_increase_bar_option=1
  autotuner_increase_bar_alpha=0.1
  
  # flags
  coordination_latency_flag=true
  conservative_service_rate_flag=false
  smooth_backlog_flag=true
  new_metrics_retriever_flag=true
  how_optimization_flag=true
  how_more_optimization_flag=false
  how_conservative_flag=false
  how_intrinsic_bound_flag=true
  scaling_decision_option=1
  lem_dp_algorithm_flag=true
  scalein_type="simple"
  is_scalein=false
  
  # migration overhead (ms)
  migration_overhead=100
  snapshot_size=1000
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -run.seconds ${runtime} \
    -base.rate ${base_rate} \
    -sine.amplitude ${sine_amplitude} \
    -sine.period ${sine_period} \
    -spike.probability ${spike_probability} \
    -spike.multiplier ${spike_multiplier} \
    -fluctuation.std ${fluctuation_std} \
    -parse.delay ${parse_delay} \
    -feature.delay ${feature_delay} \
    -scorer.base.delay ${scorer_base_delay} \
    -scorer.complexity.factor ${scorer_complexity_factor} \
    -latency.output.file ${latency_output_file} \
    -p1 ${P1} -mp1 ${MP1} \
    -p2 ${P2} -mp2 ${MP2} \
    -p3 ${P3} -mp3 ${MP3} \
    -p4 ${P4} -mp4 ${MP4} &"
    
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -run.seconds ${runtime} \
    -base.rate ${base_rate} \
    -sine.amplitude ${sine_amplitude} \
    -sine.period ${sine_period} \
    -spike.probability ${spike_probability} \
    -spike.multiplier ${spike_multiplier} \
    -fluctuation.std ${fluctuation_std} \
    -parse.delay ${parse_delay} \
    -feature.delay ${feature_delay} \
    -scorer.base.delay ${scorer_base_delay} \
    -scorer.complexity.factor ${scorer_complexity_factor} \
    -latency.output.file ${latency_output_file} \
    -p1 ${P1} -mp1 ${MP1} \
    -p2 ${P2} -mp2 ${MP2} \
    -p3 ${P3} -mp3 ${MP3} \
    -p4 ${P4} -mp4 ${MP4} &
}

function setting1(){
  # Setting 1: Light processing baseline
  printf "ML Scoring Setting 1 - Light Processing\n" >> ml_scoring_result.txt
  runtime=300
  setting="light"
  base_rate=500
  sine_amplitude=0.2
  sine_period=60.0
  spike_probability=0.02
  spike_multiplier=2.0
  fluctuation_std=0.05
  parse_delay=1
  feature_delay=1
  
  # Light processing settings
  scorer_base_delay=1
  scorer_complexity_factor=0.5  # Reduced complexity
  latency_output_file="/tmp/ml_scoring_light_latency.log"
  
  P2=1
  P3=1
  P4=1
  LP_PARSE=4
  LP_FEATURE=4
  LP_SCORER=4
  
  # Update ML config parameters
  ml_base_rate=${base_rate}
  ml_sine_amplitude=${sine_amplitude}
  ml_sine_period=${sine_period}
  ml_spike_probability=${spike_probability}
  ml_spike_multiplier=${spike_multiplier}
  ml_fluctuation_std=${fluctuation_std}
  ml_parse_delay=${parse_delay}
  ml_feature_delay=${feature_delay}
  
  for repeat in 1 2 3; do
    run_one_exp
    printf "${EXP_NAME}\n" >> ml_scoring_result.txt
  done
}

function setting2(){
  # Setting 2: Medium processing with realistic complexity
  printf "ML Scoring Setting 2 - Medium Processing\n" >> ml_scoring_result.txt
  runtime=600
  setting="medium"
  base_rate=1000
  sine_amplitude=0.4
  sine_period=120.0
  spike_probability=0.08
  spike_multiplier=4.0
  fluctuation_std=0.15
  parse_delay=2
  feature_delay=3
  
  # Medium processing settings - realistic GBDT complexity
  scorer_base_delay=3
  scorer_complexity_factor=1.5  # Moderate complexity
  latency_output_file="/tmp/ml_scoring_medium_latency.log"
  
  P2=2
  P3=2
  P4=2
  LP_PARSE=8
  LP_FEATURE=8
  LP_SCORER=8
  
  # Update ML config parameters
  ml_base_rate=${base_rate}
  ml_sine_amplitude=${sine_amplitude}
  ml_sine_period=${sine_period}
  ml_spike_probability=${spike_probability}
  ml_spike_multiplier=${spike_multiplier}
  ml_fluctuation_std=${fluctuation_std}
  ml_parse_delay=${parse_delay}
  ml_feature_delay=${feature_delay}
  
  for repeat in 1 2 3; do
    run_one_exp
    printf "${EXP_NAME}\n" >> ml_scoring_result.txt
  done
}

function setting3(){
  # Setting 3: Heavy processing - complex fraud detection
  printf "ML Scoring Setting 3 - Heavy Processing\n" >> ml_scoring_result.txt
  runtime=900
  setting="heavy"
  base_rate=1500
  sine_amplitude=0.6
  sine_period=180.0
  spike_probability=0.1
  spike_multiplier=5.0
  fluctuation_std=0.2
  parse_delay=5
  feature_delay=8
  
  # Heavy processing settings - complex fraud analysis
  scorer_base_delay=5
  scorer_complexity_factor=3.0  # High complexity for detailed analysis
  latency_output_file="/tmp/ml_scoring_heavy_latency.log"
  
  P2=4
  P3=4
  P4=4
  MP2=16
  MP3=16
  MP4=16
  LP_PARSE=16
  LP_FEATURE=16
  LP_SCORER=16
  
  # Update ML config parameters
  ml_base_rate=${base_rate}
  ml_sine_amplitude=${sine_amplitude}
  ml_sine_period=${sine_period}
  ml_spike_probability=${spike_probability}
  ml_spike_multiplier=${spike_multiplier}
  ml_fluctuation_std=${fluctuation_std}
  ml_parse_delay=${parse_delay}
  ml_feature_delay=${feature_delay}
  
  for repeat in 1 2; do
    run_one_exp
    printf "${EXP_NAME}\n" >> ml_scoring_result.txt
  done
}

function setting4(){
  # Setting 4: Variable complexity testing
  printf "ML Scoring Setting 4 - Variable Complexity\n" >> ml_scoring_result.txt
  runtime=450
  setting="variable"
  base_rate=800
  sine_amplitude=0.3
  sine_period=90.0
  spike_probability=0.05
  spike_multiplier=3.0
  fluctuation_std=0.1
  parse_delay=3
  feature_delay=4
  
  # Test different complexity factors
  for complexity in 0.5 1.0 2.0 4.0; do
    scorer_base_delay=2
    scorer_complexity_factor=${complexity}
    latency_output_file="/tmp/ml_scoring_complexity_${complexity}_latency.log"
    
    # Update ML config parameters
    ml_base_rate=${base_rate}
    ml_sine_amplitude=${sine_amplitude}
    ml_sine_period=${sine_period}
    ml_spike_probability=${spike_probability}
    ml_spike_multiplier=${spike_multiplier}
    ml_fluctuation_std=${fluctuation_std}
    ml_parse_delay=${parse_delay}
    ml_feature_delay=${feature_delay}
    
    P2=2
    P3=2
    P4=2
    LP_PARSE=8
    LP_FEATURE=8
    LP_SCORER=8
    
    run_one_exp
    printf "${EXP_NAME}\n" >> ml_scoring_result.txt
  done
}

# main execution
init

echo "Starting ML Scoring Job Cluster Experiments"
echo "Results will be stored in: ${EXP_DIR}"

# Run all settings
setting1
setting2
setting3
setting4

echo "All experiments completed. Results summary:"
cat ml_scoring_result.txt 