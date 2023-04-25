/home/swrrt11/Workspace/flinks/flink-extended/build-target/bin/flink run -c flinkapp.MultiStageLatency target/testbed-1.0-SNAPSHOT.jar -p1 3 -mp1 32 -p2 9 -mp2 32 -runTime 240 \
	-srcRate 2000 -srcPeriod 120 -srcAmplitude 1500 -srcWarmUp 20 -srcWarmupRate 2000 -srcInterval 10 -total -1 &
