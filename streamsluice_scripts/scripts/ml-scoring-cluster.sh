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
  EXP_NAME=mlscoring_${setting}-${whether_type}-${how_type}-${autotuner_initial_value_option}-${autotuner_increase_bar_option}-${autotune_interval}-${runtime}-${warmup_time}-${warmup_rate}-${P2}-${parse_delay}-${P3}-${feature_delay}-${P4}-${scorer_base_delay}-${P5}-${scorer_complexity_factor}-${L}-${autotuner_increase_bar_alpha}-${epoch}-${input_rate_factor}-${is_treat}-${migration_interval}-${conservative_factor}-${repeat}

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
  scalein_type="streamsluice"
  L=2000
  runtime=300
  warmup=10000
  warmup_time=120    # warmup duration in seconds
  warmup_rate=1000  # warmup rate txn/s
  repeat=1
  spike_estimation="linear_regression"
  spike_slope=0.7
  spike_intercept=1000
  errorcase_number=3
  calibrate_selectivity=true
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
  is_treat=true
  migration_interval=1000
  epoch=100
  decision_interval=1
  snapshot_size=20
  
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.MLmodelscoring.MLScoringJob"
  
  # ML scoring job specific parameters
  sine_baseline=1000.0      # baseline rate for sine curve f(t) = amplitude * sin(t) + baseline
  sine_amplitude=300.0      # amplitude of sine wave (absolute value, not percentage)
  sine_period=60.0          # period in seconds
  spike_probability=0.05
  spike_multiplier=3.0
  fluctuation_std=0.1
  parse_delay=1
  feature_delay=1
  input_rate_factor=1.0
  
  # Feature-based scorer configuration parameters
  scorer_base_delay=2000      # Base processing delay in microseconds
  scorer_complexity_factor=1.0 # Complexity multiplier for feature-based processing
  latency_output_file="/tmp/ml_scoring_latency.log"
  
  # parallelism settings
  P1=1
  MP1=1
  P2=1
  MP2=128
  P3=1
  MP3=128
  P4=2
  MP4=128
  P5=1
  MP5=128
  
  # ML-specific max parallelism limits for vertex-based scaling
  LP_PARSE=4     # max parallelism for parse_txn operator
  LP_FEATURE=4   # max parallelism for feature_builder operator
  LP_SCORER=30    # max parallelism for scorer operator
  
  # ML-specific configuration parameters passed to Flink config
  ml_sine_baseline=${sine_baseline}
  ml_sine_amplitude=${sine_amplitude}
  ml_sine_period=${sine_period}
  ml_spike_probability=${spike_probability}
  ml_spike_multiplier=${spike_multiplier}
  ml_fluctuation_std=${fluctuation_std}
  ml_parse_delay=${parse_delay}
  ml_feature_delay=${feature_delay}
  
  # system settings
  metrics_output=true
  autotune=true
  autotune_interval=60
  autotuner="UserLimitTuner"
  autotuner_latency_window=100
  autotuner_bar_lowerbound=350
  autotuner_adjustment_option=1
  autotuner_increase_bar_option=8
  autotuner_initial_value_alpha=1.2
  autotuner_adjustment_beta=2.0
  autotuner_initial_value_option=5
  autotuner_increase_bar_alpha=0.1
  
  # flags
  how_more_optimization_flag=false
  how_optimization_flag=false
  how_intrinsic_bound_flag=true
  how_conservative_flag=false
  coordination_latency_flag=true
  conservative_service_rate_flag=true
  conservative_factor=0.8
  transmission_delay=100
  smooth_backlog_flag=false
  new_metrics_retriever_flag=true
  scaling_decision_option=1
  is_scalein=true
  
  # migration overhead (ms)
  migration_overhead=100

  printf "" > ml_scoring_result.txt
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -run.seconds ${runtime} \
    -sine.baseline ${sine_baseline} \
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
    -warmup_time ${warmup_time} \
    -warmup_rate ${warmup_rate} \
    -input_rate_factor ${input_rate_factor} \
    -p1 ${P1} \
    -p2 ${P2} -mp2 ${MP2} \
    -p3 ${P3} -mp3 ${MP3} \
    -p4 ${P4} -mp4 ${MP4} \
    -p5 ${P5} -mp5 ${MP5} &"
    
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -run.seconds ${runtime} \
    -sine.baseline ${sine_baseline} \
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
    -warmup_time ${warmup_time} \
    -warmup_rate ${warmup_rate} \
    -input_rate_factor ${input_rate_factor} \
    -p1 ${P1} \
    -p2 ${P2} -mp2 ${MP2} \
    -p3 ${P3} -mp3 ${MP3} \
    -p4 ${P4} -mp4 ${MP4} \
    -p5 ${P5} -mp5 ${MP5} &
}

function setting1(){
  # Setting 1: Light processing baseline
  printf "ML Scoring Setting 1 - Light Processing\n" >> ml_scoring_result.txt
  runtime=600
  setting="light"
  sine_baseline=600.0         # baseline rate for sine curve
  sine_amplitude=100.0        # amplitude of sine wave (range: 500-700 txn/s)
  sine_period=60.0            # period in seconds
  warmup_rate=400             # warmup rate (should be <= sine_baseline - sine_amplitude)
  spike_probability=0.02
  spike_multiplier=2.0
  fluctuation_std=0.05
  parse_delay=100 # microseconds
  feature_delay=100  # microseconds
  
  # Light processing settings
  scorer_base_delay=1000      # 1ms in microseconds
  scorer_complexity_factor=0.5  # Reduced complexity
  latency_output_file="/tmp/ml_scoring_light_latency.log"
  
  P2=1
  P3=1
  P4=2
  P5=1

  # Update ML config parameters
  ml_sine_baseline=${sine_baseline}
  ml_sine_amplitude=${sine_amplitude}
  ml_sine_period=${sine_period}
  ml_spike_probability=${spike_probability}
  ml_spike_multiplier=${spike_multiplier}
  ml_fluctuation_std=${fluctuation_std}
  ml_parse_delay=${parse_delay}
  ml_feature_delay=${feature_delay}
  
  for repeat in 1; do # 2 3
    run_one_exp
    printf "${EXP_NAME}\n" >> ml_scoring_result.txt
  done
}

function setting2(){
  # Setting 2: Medium processing with realistic complexity
  printf "ML Scoring Setting 2 - Medium Processing\n" >> ml_scoring_result.txt
  runtime=1380
  setting="medium"
  L=2000
  transmission_delay=50
  
  sine_baseline=1500.0        # baseline rate for sine curve
  sine_amplitude=350.0        # amplitude of sine wave (range: 1100-1900 txn/s)
  sine_period=240             # period in seconds
  warmup_rate=1500            # warmup rate (should be <= sine_baseline - sine_amplitude)
  spike_probability=0.01 #0.08
  spike_multiplier=2 #4.0
  fluctuation_std=0.05 #0.15
  parse_delay=100
  feature_delay=100
  
  # Medium processing settings - realistic GBDT complexity
  scorer_base_delay=3000      # 3ms in microseconds
  scorer_complexity_factor=1.5  # Moderate complexity
  latency_output_file="/tmp/ml_scoring_medium_latency.log"
  
  P2=1
  P3=1
  P4=28
  
  # Update ML config parameters
  ml_sine_baseline=${sine_baseline}
  ml_sine_amplitude=${sine_amplitude}
  ml_sine_period=${sine_period}
  ml_spike_probability=${spike_probability}
  ml_spike_multiplier=${spike_multiplier}
  ml_fluctuation_std=${fluctuation_std}
  ml_parse_delay=${parse_delay}
  ml_feature_delay=${feature_delay}

#  for repeat in 1 2 3; do #
#    run_one_exp
#    printf "${EXP_NAME}\n" >> ml_scoring_result.txt
#  done

  printf "Comparison\n" >> ml_scoring_result.txt
  for repeat in 1; do #
    L=2000
    autotune=false
    is_treat=false
    P2=1
    P3=1
    P4=28
    whether_type="ds2"
    how_type="ds2"
    scalein_type="ds2"
    migration_interval=2500
#    run_one_exp
#    printf "${EXP_NAME}\n" >> ml_scoring_result.txt

    P2=2
    P3=3
    P4=12
    run_one_exp
    printf "${EXP_NAME}\n" >> ml_scoring_result.txt

    P2=1
    P3=1
    P4=15

#    is_treat=true
#    whether_type="ds2"
#    how_type="ds2"
#    scalein_type="ds2"
#    migration_interval=2500
#    run_one_exp
#    printf "${EXP_NAME}\n" >> ml_scoring_result.txt
#
#    whether_type="streamswitch"
#    how_type="streamswitch"
#    scalein_type="streamswitch"
#    migration_interval=2500 #1000
#    run_one_exp
#    printf "${EXP_NAME}\n" >> ml_scoring_result.txt
  done
}

function setting3(){
  # Setting 3: Heavy processing - complex fraud detection
  printf "ML Scoring Setting 3 - Heavy Processing\n" >> ml_scoring_result.txt
  runtime=1380
  setting="heavy"
  sine_baseline=2000.0        # baseline rate for sine curve
  sine_amplitude=600.0        # amplitude of sine wave (range: 1400-2600 txn/s)
  sine_period=180.0           # period in seconds
  warmup_rate=1500            # warmup rate (should be <= sine_baseline - sine_amplitude)
  spike_probability=0.1
  spike_multiplier=5.0
  fluctuation_std=0.2
  parse_delay=200
  feature_delay=200
  
  # Heavy processing settings - complex fraud analysis
  scorer_base_delay=3000      # 3ms in microseconds
  scorer_complexity_factor=3.0  # High complexity for detailed analysis
  latency_output_file="/tmp/ml_scoring_heavy_latency.log"
  
  P2=1
  P3=1
  P4=15

  # Update ML config parameters
  ml_sine_baseline=${sine_baseline}
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

function setting4(){
  # Setting 4: Variable complexity testing
  printf "ML Scoring Setting 4 - Variable Complexity\n" >> ml_scoring_result.txt
  runtime=1380
  setting="variable"
  sine_baseline=1000.0        # baseline rate for sine curve
  sine_amplitude=200.0        # amplitude of sine wave (range: 800-1200 txn/s)
  sine_period=90.0            # period in seconds
  warmup_rate=850             # warmup rate (should be <= sine_baseline - sine_amplitude)
  spike_probability=0.05
  spike_multiplier=3.0
  fluctuation_std=0.1
  parse_delay=100
  feature_delay=100
  
  # Test different complexity factors
  for complexity in 0.5 1.0 2.0 4.0; do
    scorer_base_delay=2000      # 2ms in microseconds
    scorer_complexity_factor=${complexity}
    latency_output_file="/tmp/ml_scoring_complexity_${complexity}_latency.log"
    
    # Update ML config parameters
    ml_sine_baseline=${sine_baseline}
    ml_sine_amplitude=${sine_amplitude}
    ml_sine_period=${sine_period}
    ml_spike_probability=${spike_probability}
    ml_spike_multiplier=${spike_multiplier}
    ml_fluctuation_std=${fluctuation_std}
    ml_parse_delay=${parse_delay}
    ml_feature_delay=${feature_delay}

    P2=1
    P3=1
    P4=15
    
    # Create custom EXP_NAME with complexity factor for setting 4
    setting=complexity_${complexity}

    
    run_one_exp
    printf "${EXP_NAME}\n" >> ml_scoring_result.txt
  done
}

# main execution
init

echo "Starting ML Scoring Job Cluster Experiments"
echo "Results will be stored in: ${EXP_DIR}"

# Run all settings
#setting1
setting2
#setting3
#setting4

echo "All experiments completed. Results summary:"
cat ml_scoring_result.txt 