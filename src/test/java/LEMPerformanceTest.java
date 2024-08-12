import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class LEMPerformanceTest {
    static Random rng = new Random(114514);
    public static void main(String[] args) {
        System.out.println("LEM performance testing start...");
        int n_operator = 4, n_key = 128, n_task = 20;

        // Create a dummy StreamGraph and metrics for testing
        ArrayList<String> jobs = new ArrayList<>();
        Map<String, List<String>> jobLinks = new HashMap<>();
        for(int index = 0; index < n_operator; index++){
            String operatorId = java.util.UUID.randomUUID().toString();
            jobs.add(operatorId);
            jobLinks.put(operatorId, new ArrayList<>());
            for(int j = 0; j < index; j++){
                jobLinks.get(jobs.get(j)).add(operatorId);
            }
        }
        StreamGraph graph = new StreamGraph(jobLinks);

        // Configuration
        ArrayList<String> tasks = new ArrayList<>();
        Map<String, Map<String, List<Integer>>> config = new HashMap<>();
        for(int index = 0; index < n_operator; index++) {
            String operatorId = jobs.get(index);
            Map<String, List<Integer>> mapping = new HashMap<>();
            config.put(operatorId, mapping);
            int current_k = 0, average = n_key / n_task, remainder = n_key % n_task;
            for (int j = 0; j < n_task; j++) {
                String task = operatorId + "_" + j;
                tasks.add(task);
                List<Integer> keys = new ArrayList<>();
                mapping.put(task, keys);
                int end_k = current_k + average;
                if(j < remainder){
                    end_k ++;
                }
                for(int k = current_k; k < end_k; k++){
                    keys.add(k);
                }
                current_k = end_k;
            }
        }

        // Metrics
        Map<String, Map<Integer, Double>> arrivalRatesPerKey = new HashMap<>();
        Map<String, Map<Integer, Double>> backlogPerKey = new HashMap<>();
        for(int index = 0; index < n_operator; index ++){
            Map<Integer, Double>
        }
        Map<String, Double> serviceRatesPerTask = new HashMap<>();
        Map<String, Double> waitingTimePerTask = new HashMap<>();
        // Populate these maps with some dummy data
        // This is just placeholder data for testing purposes

        double conservativeFactor = 0.875;
        LatencyEstimationModel lem = new LatencyEstimationModel(graph, arrivalRatesPerKey, backlogPerKey, serviceRatesPerTask, waitingTimePerTask, conservativeFactor);

        // Example task, operator, and config for testing
        String task = "task1";
        String operator = "operator1";
        List<Integer> mappedKeys = Arrays.asList(0, 1);
        double time = 10.0;

        // Measure the time taken for estimateTaskLatency
        long startTime = System.nanoTime();
        double taskLatency = lem.estimateTaskLatency(time, task, operator, mappedKeys);
        long endTime = System.nanoTime();
        System.out.println("Task Latency: " + taskLatency);
        System.out.println("Time taken for estimateTaskLatency: " + (endTime - startTime) / 1_000_000.0 + " ms");

        // Measure the time taken for estimateOperatorLatency
        Map<String, List<Integer>> operatorConfig = new HashMap<>();
        operatorConfig.put(task, mappedKeys);

        startTime = System.nanoTime();
        double operatorLatency = lem.estimateOperatorLatency(time, operator, operatorConfig);
        endTime = System.nanoTime();
        System.out.println("Operator Latency: " + operatorLatency);
        System.out.println("Time taken for estimateOperatorLatency: " + (endTime - startTime) / 1_000_000.0 + " ms");

        // Measure the time taken for estimateEndToEndLatency
        Map<String, Map<String, List<Integer>>> endToEndConfig = new HashMap<>();
        endToEndConfig.put(operator, operatorConfig);

        startTime = System.nanoTime();
        double endToEndLatency = lem.estimateEndToEndLatency(time, endToEndConfig);
        endTime = System.nanoTime();
        System.out.println("End-to-End Latency: " + endToEndLatency);
        System.out.println("Time taken for estimateEndToEndLatency: " + (endTime - startTime) / 1_000_000.0 + " ms");
    }
}

class StreamGraph {
    private final List<String> orderedOperator;
    private final int [][] adjacentMatrix;
    private int depth;
    public StreamGraph(Map<String, List<String>> operatorLinks){
        orderedOperator = new ArrayList<>();
        adjacentMatrix = new int[operatorLinks.size()][operatorLinks.size()];
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, Integer> index = new HashMap<>();
        for(String operator: operatorLinks.keySet()){
            if(!indegree.containsKey(operator)){
                indegree.put(operator, 0);
            }
            for(String tOperator: operatorLinks.get(operator)){
                indegree.put(tOperator, indegree.getOrDefault(tOperator, 0) + 1);
            }
        }

        Map<String, Integer> operatorDepth = new HashMap<>();
        while(indegree.size() > 0){
            String tOperator = null;
            for(String operator: indegree.keySet()){
                if(indegree.get(operator) == 0){
                    tOperator = operator;
                    break;
                }
            }
            if(!operatorDepth.containsKey(tOperator)){
                operatorDepth.put(tOperator, 1);
            }
            assert (tOperator != null);
            for(String operator: operatorLinks.get(tOperator)){
                if(!operatorDepth.containsKey(operator)){
                    operatorDepth.put(operator, operatorDepth.get(tOperator) + 1);
                }
                indegree.put(operator, indegree.get(operator) - 1);
            }
            index.put(tOperator, orderedOperator.size());
            orderedOperator.add(tOperator);
            indegree.remove(tOperator);
        }
        depth = 0;
        for(String operator: operatorLinks.keySet()){
            if(operatorDepth.get(operator) > depth){
                depth = operatorDepth.get(operator);
            }
            for(String tOperator: operatorLinks.get(operator)){
                adjacentMatrix[index.get(operator)][index.get(tOperator)] = 1;
            }
        }
    }
    public List<String> getOrderedOperator(){
        return orderedOperator;
    }
    public List<String> getUpstreamOperators(String operator) {
        List<String> upstreamOperators = new LinkedList<>();
        int index = 0;
        for (int i = 0; i < orderedOperator.size(); i++) {
            if (orderedOperator.get(i).equals(operator)) {
                index = i;
            }
        }
        for (int i = 0; i < orderedOperator.size(); i++) {
            if (adjacentMatrix[i][index] == 1) {
                upstreamOperators.add(orderedOperator.get(i));
            }
        }
        return upstreamOperators;
    }
    public List<String> getDownstreamOperators(String operator) {
        List<String> downstreamOperators = new LinkedList<>();
        int index = 0;
        for (int i = 0; i < orderedOperator.size(); i++) {
            if (orderedOperator.get(i).equals(operator)) {
                index = i;
            }
        }
        for (int i = 0; i < orderedOperator.size(); i++) {
            if (adjacentMatrix[index][i] == 1) {
                downstreamOperators.add(orderedOperator.get(i));
            }
        }
        return downstreamOperators;
    }

    public int getGraphDepth(){
        return depth;
    }
}



class LatencyEstimationModel {
    private static final Logger LOG = LoggerFactory.getLogger(LatencyEstimationModel.class);

    final StreamGraph graph;
    protected class Metrics{
        Map<String, Map<Integer, Double>> arrivalRatePerKey, backlogPerKey;
        Map<String, Double> serviceRatePerTask, waitTimePerTask;

        Metrics(Map<String, Map<Integer, Double>> _arrival, Map<String, Map<Integer, Double>> _backlog, Map<String, Double> _service, Map<String, Double> _waitTime){
            arrivalRatePerKey = _arrival;
            backlogPerKey = _backlog;
            serviceRatePerTask = _service;
            waitTimePerTask = _waitTime;
        }
    }

    final Metrics metrics;
    final double conservativeFactor;
    private double bottleneckBound = -1.0;
    private boolean bottleneckBoundSet = false;
    private Map<String, Double> startTimePerOperator;
    public LatencyEstimationModel(StreamGraph _graph,
                                  Map<String, Map<Integer, Double>> arrivalRatesPerKey,
                                  Map<String, Map<Integer, Double>> backlogPerKey,
                                  Map<String, Double> serviceRatesPerTask,
                                  Map<String, Double> waitingTimePerTask,
                                  double _conservativeFactor){
        graph = _graph;
        conservativeFactor = _conservativeFactor;
        metrics = new Metrics(arrivalRatesPerKey, backlogPerKey, serviceRatesPerTask, waitingTimePerTask);
    }

    public double estimateTaskLatency(double time, String task, String operator, List<Integer> mappedKeys){
        // long estimationStartTime = System.nanoTime();
        // LOG.info("+++ [MODEL] task start nanotime: " + estimationStartTime);
        double serviceRate;
        if(metrics.serviceRatePerTask.containsKey(task)){
            serviceRate = metrics.serviceRatePerTask.get(task);
        }else{
            // Calculate service rate for new tasks.
            double operatorTotalServiceRate = 0;
            int operatorTaskNumber = 0;
            for(String target_task: metrics.serviceRatePerTask.keySet()){
                if(target_task.startsWith(operator)){
                    operatorTaskNumber +=1;
                    operatorTotalServiceRate += metrics.serviceRatePerTask.get(target_task);
                }
            }
            serviceRate = operatorTotalServiceRate / operatorTaskNumber;
        }
        // long serviceEndTime = System.nanoTime();
        // LOG.info("+++ [MODEL] s_rate end nanotime: " + serviceEndTime + ", time: " + (serviceEndTime - estimationStartTime));

        double arrivalRate = 0, backlog = 0;
        for(int key: mappedKeys){
            arrivalRate += metrics.arrivalRatePerKey.get(operator).get(key);
            backlog += metrics.backlogPerKey.get(operator).get(key);
        }
        backlog = Math.max(backlog + (arrivalRate - serviceRate) * time, 0);
        // long abEndTime = System.nanoTime();
        // LOG.info("+++ [MODEL] a_rate backlog end nanotime: " + abEndTime + ", time: " + (abEndTime - serviceEndTime));

        double waitingTime = 0;
        if(metrics.waitTimePerTask.containsKey(task)){
            waitingTime = metrics.waitTimePerTask.get(task);
        }else{
            // Calculate waiting time for new tasks.
            double operatorTotalWaitingTime = 0;
            int operatorTaskNumber = 0;
            for(String target_task: metrics.waitTimePerTask.keySet()){
                if(target_task.startsWith(operator)){
                    operatorTaskNumber +=1;
                    operatorTotalWaitingTime += metrics.waitTimePerTask.get(target_task);
                }
            }
            waitingTime = operatorTotalWaitingTime / operatorTaskNumber;
        }
        // long waitEndTime = System.nanoTime();
        // LOG.info("+++ [MODEL] wait_time end nanotime: " + waitEndTime + ", time: " + (waitEndTime - abEndTime));

        double result = (backlog + 1) / serviceRate + waitingTime;
        //long estimationEndTime = System.nanoTime();
        // LOG.info("+++ [MODEL] task end nanotime: " + estimationEndTime + ", total time: " + (estimationEndTime - estimationStartTime));
        return result;
    }
    public double estimateOperatorLatency(double time, String operator, Map<String, List<Integer>> config){
        //long estimationStartTime = System.nanoTime();
        //LOG.info("+++ [MODEL] op start nanotime: " + estimationStartTime);
        double maxLatency = 0;
        for(String task: config.keySet()){
            double latency = estimateTaskLatency(time, task, operator, config.get(task));
            maxLatency = Math.max(maxLatency, latency);
        }
        //long estimationEndTime = System.nanoTime();
        //LOG.info("+++ [MODEL] op end nanotime: " + estimationEndTime + ", total time: " + (estimationEndTime - estimationStartTime));
        return maxLatency;
    }
    public double estimateEndToEndLatency(double time, Map<String, Map<String, List<Integer>>> config){
        long estimationStartTime = System.nanoTime();
        // LOG.info("+++ [MODEL] ete start nanotime: " + estimationStartTime);
        Map<String, Double> endTime = new HashMap<>();
        for(String operator: graph.getOrderedOperator()){
            long operatorStartTime = System.nanoTime();
            // LOG.info("+++ [MODEL] operator start nanotime: " + operatorStartTime);
            double startTime = time;
            for(String upstreamOperator: graph.getUpstreamOperators(operator)){
                startTime = Math.max(startTime, endTime.get(upstreamOperator));
            }
//			long operatorMaxTime = System.nanoTime();
//			LOG.info("+++ [MODEL] operator max nanotime: " + (operatorMaxTime - operatorStartTime));

            double latency = estimateOperatorLatency(startTime, operator, config.get(operator));
            endTime.put(operator, startTime + latency);
//			long operatorEndTime = System.nanoTime();
//			LOG.info("+++ [MODEL] operator total nanotime: " + (operatorEndTime - operatorStartTime));
        }
        double maxLatency = 0;
        for(String operator: endTime.keySet()){
            maxLatency = Math.max(maxLatency, endTime.get(operator) - time);
        }
        System.out.println("+++ [MODEL] operators endTime:" + endTime);
//		long estimationEndTime = System.nanoTime();
//		LOG.info("+++ [MODEL] ete end total time: " + (estimationEndTime - estimationStartTime));
        return maxLatency;
    }

    public int getMaxFutureBacklogKey(String operator, double startTime, List<Integer> keys){
        int maxKey = keys.get(0);
        double maxBacklog = -1;
        for(int key: keys){
            double newBacklog = metrics.backlogPerKey.get(operator).get(key) + startTime * metrics.arrivalRatePerKey.get(operator).get(key);
            if(newBacklog > maxBacklog){
                maxBacklog = newBacklog;
                maxKey = key;
            }
        }
        return maxKey;
    }

    public double getTaskMaxTupleCanProcess(String operator, String task, double availableTimeToProcess){
        double serviceRate = metrics.serviceRatePerTask.get(task);
        return (availableTimeToProcess - metrics.waitTimePerTask.get(task)) * serviceRate;
    }

    public double getTaskTrueServiceRate(String task) {
        return metrics.serviceRatePerTask.get(task);
    }

    public double getTaskConservativeServiceRate(String operator, String task) {
        return metrics.serviceRatePerTask.get(task) * conservativeFactor;
    }

    public double getKeyTupleToProcess(String operator, int key, double starTime){
        double arrivalRate = metrics.arrivalRatePerKey.get(operator).get(key);
        double backlog = metrics.backlogPerKey.get(operator).get(key);
        return Math.max(starTime * arrivalRate + backlog, 0.0);
    }

    public double getKeyArrivalRate(String operator, int key){
        return metrics.arrivalRatePerKey.get(operator).get(key);
    }

    public boolean isTaskOverloaded(String task, String operator, List<Integer> mappedKeys){
        double serviceRate = 0;
        if(metrics.serviceRatePerTask.containsKey(task)){
            serviceRate = metrics.serviceRatePerTask.get(task);
        }else{
            // Calculate service rate for new tasks.
            double operatorTotalServiceRate = 0;
            int operatorTaskNumber = 0;
            for(String target_task: metrics.serviceRatePerTask.keySet()){
                if(target_task.startsWith(operator)){
                    operatorTaskNumber +=1;
                    operatorTotalServiceRate += metrics.serviceRatePerTask.get(target_task);
                }
            }
            serviceRate = operatorTotalServiceRate / operatorTaskNumber;
        }
        double arrivalRate = 0;
        for(int key: mappedKeys){
            arrivalRate += metrics.arrivalRatePerKey.get(operator).get(key);
        }
        LOG.info("+++ [MODEL] task rate " + task + " (arrival, service): " + arrivalRate + ", " + serviceRate);
        return arrivalRate > serviceRate * conservativeFactor;
    }

    public boolean checkLatencyBoundCondition(String bottleneckOperator, double latencyBound, Map<String, Map<String, List<Integer>>> config){
        if(!bottleneckBoundSet){
            startTimePerOperator = new HashMap<>();
            Map<String, Double> completeTimePerOperator = new HashMap<>(), deadlineStartTimePerOperator = new HashMap<>(), deadlineCompleteTimePerOperator = new HashMap<>();
            for(String operator: graph.getOrderedOperator()) {
                double startTime = 0;
                for (String upstream : graph.getUpstreamOperators(operator)) {
                    startTime = Math.max(startTime, completeTimePerOperator.get(upstream));
                }

                double completeTime = startTime + this.estimateOperatorLatency(startTime, operator, config.get(operator));
                startTimePerOperator.put(operator, startTime);
                completeTimePerOperator.put(operator, completeTime);
            }
            LOG.info("Operators' start and complete time: " + startTimePerOperator + "   " + completeTimePerOperator);

            List<String> reverseOrder = new ArrayList<String>(graph.getOrderedOperator());
            Collections.reverse(reverseOrder);
            for(String operator: reverseOrder){
                double deadlineCompleteTime = latencyBound;
                for(String downstream: graph.getDownstreamOperators(operator)){
                    deadlineCompleteTime = Math.min(deadlineCompleteTime, deadlineStartTimePerOperator.get(downstream));
                }

                // Find deadline start time
                double upperbound = deadlineCompleteTime, lowerbound = 0;
                for(int times = 0; times <= 50; times ++){
                    double mid = (upperbound + lowerbound) / 2;
                    double endTime = mid + this.estimateOperatorLatency(mid, operator, config.get(operator));
                    if(endTime >= deadlineCompleteTime){
                        upperbound = mid;
                    }else{
                        lowerbound = mid;
                    }
                }
                double deadlineStartTime = lowerbound;
                deadlineStartTimePerOperator.put(operator, deadlineStartTime);
                deadlineCompleteTimePerOperator.put(operator, deadlineCompleteTime);
            }
            bottleneckBound = deadlineCompleteTimePerOperator.get(bottleneckOperator) - startTimePerOperator.get(bottleneckOperator);
            bottleneckBoundSet = true;
        }

        for(String task: config.get(bottleneckOperator).keySet()){
            if(estimateTaskLatency(startTimePerOperator.get(bottleneckOperator), task, bottleneckOperator, config.get(bottleneckOperator).get(task)) > bottleneckBound){
                return false;
            }
        }
        return true;
    }

    public double getTaskWaitingTime(String operator, String task){
        return metrics.waitTimePerTask.get(task);
    }
}