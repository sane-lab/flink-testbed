<p align="left">
  <a href="https://github.com/yancanmao/flink-testbed/actions">
    <img alt="GitHub Actions status" src="https://github.com/yancanmao/flink-testbed/workflows/Java%20CI/badge.svg"></a>
</p>

# flink-testbed

### Test merge operator:
1. Clone Flink:
`git clone https://github.com/yancanmao/flink-extended.git`
2. Checkout Flink branch:
`git checkout debug_merge_operator`
3. Compile Flink:
`mvn clean install -DskipTests -Dcheckstyle.skip -Drat.skip=true -e`
4. Clone testbed:
`git clone https://github.com/sane-lab/flink-testbed.git`
5. Checkout testbed branch:
`git checkout streamsluice`
6. Compile testbed:
`mvn clean package`
7. Run application:
`/home/samza/workspace/flink-related/flink-extended-ete/build-target/bin/flink run -c flinkapp.StreamSluiceTestSet.DAGTest /home/samza/workspace/flink-related/flink-testbed-sane/target/testbed-1.0-SNAPSHOT.jar     -p1 1 -mp1  -p2 3 -mp2 128 -op2Delay 250 -op2IoRate 1 -op2KeyStateSize 1000     -p3 7 -mp3 128 -op3Delay 800 -op3IoRate 1 -op3KeyStateSize 1000 -p4 4 -mp4 128 -op4Delay 400 -op4IoRate 1 -op4KeyStateSize 1000     -p5 3 -mp5 128 -op5Delay 125 -op5KeyStateSize 1000     -nkeys 1000 -phase1Time 600 -phase1Rate 6000 -phase2Time 40 -phase2Rate 8000 -interTime 120 -interRate 7000 -interPeriod 240 -zipf_skew 0`