scp_from_dir(){
	name="${host}:${inputdir}/flink-samza-standalonesession-0-camel-sane.out" #*.out"
	echo $name
	expName=$(basename ${inputdir})
	mkdir "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/${expName}"
	scp -r "$name" "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/${expName}"
}
host="samza@camel-sane.d2.comp.nus.edu.sg" #
#host="samza@eagle-sane.d2.comp.nus.edu.sg" #"samza@flamingo-sane.d2.comp.nus.edu.sg" #  #
path="/data/streamsluice/raw/"
setting="-120-400-600-500-120-2-0-1000-500-80000-100" #"-180-400-400-500-30-5-10-2-0.25-750-500-10000-100" #
exp="streamsluice-2opscaleout" #"streamsluice-twoOP" #  # "streamsluice-scaleout" #
#declare -a dir=(
#"/data/streamsluice/raw/streamsluice-4op-300-5000-5000-10000-120-1-0-2-200-1-100-5-500-1-100-3-333-1-100-3-250-100-1000-600-100-true-1"
#"/data/streamsluice/raw/streamsluice-4op-300-8000-8000-10000-120-1-0-2-200-1-100-5-500-1-100-3-333-1-100-3-250-100-1000-600-100-true-1"
#"/data/streamsluice/raw/streamsluice-dag-120-300-600-450-240-1-0.25-1-1000-1-1-4-10000-1000-1-1000-1000-1000-500-100-true-1"
#"${path}${exp}${setting}-true-1"
#"${path}${exp}-streamsluice-streamsluice${setting}-false-1"
#"${path}${exp}-streamsluice-streamsluice${setting}-true-1"
#"${path}${exp}-streamsluice_threshold25-streamsluice${setting}-true-1"
#"${path}${exp}-streamsluice_threshold50-streamsluice${setting}-true-1"
#"${path}${exp}-streamsluice_threshold75-streamsluice${setting}-true-1"
#"${path}${exp}-streamsluice_threshold100-streamsluice${setting}-true-1"
#"/data/streamsluice/raw/streamsluice-scaleout-streamsluice-streamsluice-120-400-400-450-10-5-0-1000-500-10000-100-false-1"
#"/data/streamsluice/raw/streamsluice-scaleout-streamsluice-streamsluice-120-400-400-450-10-5-0-1000-500-10000-100-true-1"
#"/data/streamsluice/raw/streamsluice-scaleout-streamsluice_threshold25-streamsluice-120-400-400-450-10-5-0-1000-500-10000-100-true-1"
#"/data/streamsluice/raw/streamsluice-scaleout-streamsluice_threshold50-streamsluice-120-400-400-450-10-5-0-1000-500-10000-100-true-1"
#"/data/streamsluice/raw/streamsluice-scaleout-streamsluice_threshold75-streamsluice-120-400-400-450-10-5-0-1000-500-10000-100-true-1"
#"/data/streamsluice/raw/streamsluice-scaleout-streamsluice_threshold100-streamsluice-120-400-400-450-10-5-0-1000-500-10000-100-true-1"
#)
#for inputdir in ${dir[@]}; do
file="inputList.txt"
while IFS= read -r line; do
  printf 'Line: %s\n' "$line"
  trap "echo Exited!; exit;" SIGINT SIGTERM
  inputdir="/data/streamsluice/raw/${line}"
	scp_from_dir
	#python ./draw/RatesPerJob.py ${inputdir}
	#python ./draw/GroundTruthCurveAndSuccessRate.py ${inputdir}
	#python ./draw/ParallelismCurve.py ${inputdir}
	#python ./draw/RetrieveResults.py ${inputdir}
done < $file

