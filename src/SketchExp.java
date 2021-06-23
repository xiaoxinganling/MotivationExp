import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.List;
import static java.lang.System.exit;

public class SketchExp {
    public static int MAXITERATION = 2000000;//14295731;
    private static String multiPath = "multi/sketch_res";
    //private static String fileName = "C:\\Users\\xiaoxinganling\\Desktop\\batch_task.csv";
    private static int MAXOUT = 4;
    private static int MAXSTEP = 4;
    public static void main(String[] args) throws Exception {
        if(args.length != 5){
            System.err.println("Usage: tracePath multiPath maxOut maxStep maxIteration");
            exit(-1);
        }
        String fileName = args[0];
        multiPath = args[1];
        MAXOUT = Integer.valueOf(args[2]);
        MAXSTEP = Integer.valueOf(args[3]);
        MAXITERATION = Integer.valueOf(args[4]);
        // write job description
        Map<String, Job> jobs = generateJobsWithIteration(fileName, MAXITERATION);
        //writeJobDescription(jobs);
        // test one job
//        List<String> keys = new ArrayList<>(jobs.keySet());
//        keys.sort((o1, o2) -> jobs.get(o1).startTime.subtract(jobs.get(o2).startTime).intValue());
//        Map<String, Job> map = new HashMap<>();//应该放在循环外
//        for(String k : keys){
//            Job cur = jobs.get(k);
//            String endTask = getEndTask(getGraph(cur));
//            if(getMaxStep(cur, endTask) == 6 && getMaxOutDegree(getGraph(cur)) >= 4){
//                //System.out.println(cur.jobName);
//                map.put(k, jobs.get(k));
//            }
//            if(map.size() == 5){
//                System.out.println("before generating...");
//                multiJobExp(map,"test");
//                break;
//            }
//        }
         //test multiply job
        multiJobExp(jobs, multiPath);
        // test dynamic multiply job
    }
    private static void multiJobExp(Map<String, Job> jobs, String fileName) throws Exception{
        Map<Integer, List<Double>> outDegreeTimeAvg = new HashMap<>();
        Map<Integer, List<Double>> outDegreeMemAvg = new HashMap<>();
        Map<Integer, List<Double>> stepTimeAvg = new HashMap<>();
        Map<Integer, List<Double>> stepMemAvg = new HashMap<>();
        Map<Integer, Integer> outDegreeMap = new HashMap<>();
        Map<Integer, Integer> stepMap = new HashMap<>();
        // filtered jobs
        int curNum = 0;
        List<Job> filteredJobs = new ArrayList<>();
        for(Job j : jobs.values()){
            Map<String, GNode> graph = getGraph(j);
            String endTask = getEndTask(graph);
            int maxOutDegree = getMaxOutDegree(graph);
            int maxStep = getMaxStep(j, endTask);
            if(maxOutDegree >= MAXOUT && maxStep >= MAXSTEP){
                outDegreeMap.put(maxOutDegree, outDegreeMap.getOrDefault(maxOutDegree, 0) + 1);
                stepMap.put(maxStep, stepMap.getOrDefault(maxStep, 0) + 1);
                filteredJobs.add(j);
            }
            curNum++;
            if(curNum % 10000 == 0){
                System.out.println(curNum + "/" + MAXITERATION);
            }
        }
        curNum = 0;
        for(Job j : filteredJobs){
            // 0 => out degree's decreased time
            // 1 => out degree's memory consumption
            // 2 => step's decreased time
            // 3 => step's memory consumption
            List<List<Double>> list = singleJobExp(j, "");
            curNum++;
            if(curNum % 1000 == 0){
                System.out.println(curNum + "/" + MAXITERATION);
            }
            updateAvg(outDegreeTimeAvg, outDegreeMap, list.get(0));
            updateAvg(outDegreeMemAvg, outDegreeMap, list.get(1));
            updateAvg(stepTimeAvg, stepMap, list.get(2));
            updateAvg(stepMemAvg, stepMap, list.get(3));
        }
        System.out.println("filterd Job size: " + filteredJobs.size());
        System.out.println("out degree " + outDegreeMap);
        System.out.println("step " + stepMap);
        int check = 0;
        for(int i : outDegreeMap.values()){
            check += i;
        }
        System.out.println("check size " + check);
        for(int i : stepMap.values()){
            check += i;
        }
        System.out.println("check size " + check);
        // print file
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName + "_out_degree"));
        BufferedWriter bw2 = new BufferedWriter(new FileWriter(fileName + "_step"));
        writeTimeAndMem(bw, outDegreeTimeAvg, outDegreeMemAvg);
        writeTimeAndMem(bw2, stepTimeAvg, stepMemAvg);
        bw.close();
        bw2.close();

//        int curNum = 0;
//        for(Job j : jobs.values()){
//            if(singleJobExp(j, multiPath + "_" + curNum)){
//                System.out.println(curNum++);
//            }
//
////            if(curNum == 10){
////                break;
////            }
//        }
    }
    // 打印decreased time和memory
    private static void writeTimeAndMem(BufferedWriter bw, Map<Integer, List<Double>> decreasedTime, Map<Integer, List<Double>> mem) throws IOException {
        if(decreasedTime.size() != mem.size()){
            System.err.println("size isn't equal: error!!!!!");
        }
        List<Integer> keys = new ArrayList<>(decreasedTime.keySet());
        keys.sort((Comparator.comparingInt(o -> o)));
        for(Integer key : keys){
            bw.write(key + "\n");
            bw.write(decreasedTime.get(key) + "\n");
            bw.write(mem.get(key) + "\n");
        }
    }
    // 计算平均值存入map
    private static void updateAvg(Map<Integer, List<Double>> map, Map<Integer, Integer> dict, List<Double> value){
//        for(double i : value){
//            if(i < 0){
//                return;//<0直接不统计
//            }
//        }
        int key = value.size();
        // 查表看有多少总数
        int total = dict.get(key);
        // 更新当前value
        for(int i = 0; i < value.size(); i++){
            value.set(i, value.get(i) / total);
        }
        // 累加到map中
        if(!map.containsKey(key)){
            map.put(key, value);
        }else{
            if(map.get(key).size() != value.size()){
                System.err.println("error!!!!");
            }
            List<Double> tmp = map.get(key);
            for(int i = 0; i < tmp.size(); i++){
//                if(tmp.get(i) < 0 || value.get(i) < 0 || tmp.get(i) + value.get(i) < 0){
//                    System.out.println("<<<<<<<<<<<<<<" + tmp.get(i) + " " + value.get(i));
//                    System.out.println("to be update: " + map.get(key));
//                    System.out.println("to be add: " + value);
//                    System.out.println("key: " + key);
//                }
                tmp.set(i, tmp.get(i) + value.get(i));
            }
        }
    }
    private static List<List<Double>> singleJobExp(Job j, String fileName) throws Exception{
//                Map<String, GNode> graph = getGraph(res.get(k));
//                for(String key : graph.keySet()){
//                    GNode gNode = graph.get(key);
//                    System.out.println(gNode.id + " " + gNode.inDegree + " " + gNode.outDegree);
//                }
//                System.out.println(getEndTask(graph));
//                for(int i = 1; i < 14; i++){
//                    System.out.println(i + " " + getTasksWithStep(res.get(k), i, getEndTask(graph)));
//                }
//                for(int i = 1; i < 10; i++) {
//                    System.out.println(i + " " + getTasksWithOutdegree(res.get(k), i));
//                }
//                Set<String> cache = getTasksWithOutdegree(res.get(k), 4);
//                System.out.println(getSizeWithCache(res.get(k), cache));
//                System.out.println(getTimeWithCache(res.get(k),cache,getEndTask(graph)));
        List<List<Double>> res = new ArrayList<>();
        Map<String, GNode> graph = getGraph(j);
        String endTask = getEndTask(graph);
        int maxOutDegree = getMaxOutDegree(graph);
        int maxStep = getMaxStep(j, endTask);
        System.out.println(maxOutDegree + " degree-step " + maxStep);
        if(maxOutDegree < MAXOUT || maxStep < MAXSTEP){
            return null;
        }
        //BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
        // 随着出边数的增加，cache收益和内存占用的变化
        List<Double> decreaseTime = new ArrayList<>();
        List<Double> memoryConsumption = new ArrayList<>();
        for(int i = 1; i <= maxOutDegree; i++){
            Set<String> cache = getTasksWithOutdegree(j, i);
            decreaseTime.add(getTimeWithCache(j,cache,endTask).doubleValue());
            memoryConsumption.add(getSizeWithCache(j, cache));
        }
        System.out.println(decreaseTime);
        System.out.println(memoryConsumption);
        // 随着距离action远近的增加，cache收益和内存占用的变化
        List<Double> stepDecreaseTime = new ArrayList<>();
        List<Double> stepMemoryConsumption = new ArrayList<>();
        for(int i = 1; i <= maxStep; i++){
            Set<String> cache = getTasksWithStep(j,i,endTask);
            stepDecreaseTime.add(getTimeWithCache(j,cache,endTask).doubleValue());
            stepMemoryConsumption.add(getSizeWithCache(j, cache));
        }
        System.out.println(stepDecreaseTime);
        System.out.println(stepMemoryConsumption);
        double averageMem = 0;
        for(double d : memoryConsumption){
            averageMem += d;
        }
        System.out.println("avg memory consumption: " + averageMem / memoryConsumption.size());
//        bw.write(decreaseTime.size() + "\n");
//        bw.write(stepDecreaseTime.size() + "\n");
//        bw.write(decreaseTime + "\n");
//        bw.write(memoryConsumption + "\n");
//        bw.write(stepDecreaseTime + "\n");
//        bw.write(stepMemoryConsumption + "\n");
        //bw.close();
//        if(decreaseTime.get(0) < 0 || stepDecreaseTime.get(0) < 0){
//            System.out.println(j);
//            for(Task t : j.tasks){
//                System.out.println(t);
//            }
//            System.out.println("not true!!!!!!!! " + decreaseTime  + memoryConsumption + stepDecreaseTime + stepMemoryConsumption);
//        }
        res.add(decreaseTime);
        res.add(memoryConsumption);
        res.add(stepDecreaseTime);
        res.add(stepMemoryConsumption);
        return res;
    }
    // get max step
    public static int getMaxStep(Job j, String endTask){
        Map<String, Task> tasks = new HashMap<>();
        for(Task t : j.tasks){
            tasks.put(t.taskId, t);
        }
        Queue<Task> queue = new LinkedList<>();
        queue.offer(tasks.get(endTask));
        int step = -1; //endTask的step为0
        while(!queue.isEmpty()){
            int size = queue.size();
            DynamicAdjustExp.oneStep(tasks, queue, size);
            step++;
        }
        return step;
    }
    // get max outDegree
    public static int getMaxOutDegree(Map<String, GNode> graph){
        int res = Integer.MIN_VALUE;
        for(GNode gnode : graph.values()){
            res = Math.max(res, gnode.outDegree);
        }
        return res;
    }
    // write job description
    private static void writeJobDescription(Map<String, Job> jobs) throws Exception {
        List<String> keys = new ArrayList<>(jobs.keySet());
        keys.sort((o1, o2) -> jobs.get(o1).startTime.subtract(jobs.get(o2).startTime).intValue());
        System.out.println(keys.size());
        writeFile(keys, jobs, "job_description");
    }
    // for every job
    // 1. get execution time with cached tasks
    public static BigInteger getTimeWithCache(Job j, Set<String> cache, String endTask){
        Map<String, Task> tasks = new HashMap<>();
        for(Task t : j.tasks){
            tasks.put(t.taskId, t);
        }
        BigInteger before = BigInteger.valueOf(0);
        for(Task t : j.tasks){
//            if(t.endTime.subtract(t.startTime).compareTo(BigInteger.valueOf(0)) < 0 ){
//                return BigInteger.valueOf(0);//防止出现endtime < start time的情况
//            }
            before = before.add(t.endTime.subtract(t.startTime));
        }
        Set<String> needToCalculate = new HashSet<>();
        Queue<Task> queue = new LinkedList<>();
        queue.offer(tasks.get(endTask));
        needToCalculate.add(endTask);
        DynamicAdjustExp.getTasksNeedToCalculate(cache, tasks, needToCalculate, queue);
        BigInteger after = BigInteger.valueOf(0);
        for(Task t : j.tasks){
            //System.out.println(t.taskId + " " + needToCalculate + " " + needToCalculate.contains(t.taskId));
            if(needToCalculate.contains(t.taskId)){
                //System.out.println(t.endTime + " " + t.startTime + " " + t.endTime.subtract(t.startTime));
                after = after.add(t.endTime.subtract(t.startTime));
            }
        }
        //System.out.println(needToCalculate);
        //System.out.println(before);
        return before.subtract(after);
    }

    // 2. get tasks with outdegree
    public static Set<String> getTasksWithOutdegree(Job j, int outDegree){
        Set<String> res = new HashSet<>();
        Map<String, GNode> graph = getGraph(j);
        for(String key : graph.keySet()){
            if(graph.get(key).outDegree == outDegree){
                res.add(key);
            }
        }
        return res;
    }

    // 3. get tasks with step
    public static Set<String> getTasksWithStep(Job j, int step, String endTask){
        Map<String, Task> tasks = new HashMap<>();
        for(Task t : j.tasks){
            tasks.put(t.taskId, t);
        }
        Queue<Task> queue = new LinkedList<>();
        queue.offer(tasks.get(endTask));
        DynamicAdjustExp.getTasksWithStepByQueue(step, tasks, queue);
        Set<String> res = new HashSet<>();
        while(!queue.isEmpty()){
            Task cur = queue.poll();
            if(cur == null){
                continue;
            }
            res.add(cur.taskId);
        }
        return res;
    }

    // 4. get total size with chached tasks
    public static double getSizeWithCache(Job j, Set<String> cache){
        double res = 0;
        for(Task t : j.tasks){
            if(cache.contains(t.taskId)){
                res += t.memorySize;
            }
        }
        return res;
    }

    // 5. get graph with jobs
    public static Map<String, GNode> getGraph(Job j){
        Map<String, GNode> graph = new HashMap<>();
        for(Task t: j.tasks){
            GNode child = graph.getOrDefault(t.taskId, new GNode(0, 0, t.taskId));
            for(String tp : t.parents){
                GNode parent = graph.getOrDefault(tp, new GNode(0,0, tp));
                parent.outDegree++;
                child.inDegree++;
                graph.put(tp, parent);
            }
            graph.put(t.taskId, child);
        }
        return graph;
    }
    // 6. get end task
    public static String getEndTask(Map<String, GNode> graph){
        for(String key : graph.keySet()){
            if(graph.get(key).outDegree == 0){
                return key;
            }
        }
        return null;
    }

    public static Map<String, Job> generateJobsWithIteration(String fileName, int iteration) throws Exception {
        Map<String, Job> res = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String line;
        int curNum = 0;
        while ((line = br.readLine()) != null) {
            String[] attrs = line.split(",");
            if(attrs.length != 9){
                System.out.println(line);
                continue;
            }
            Task task = new Task(attrs[0], BigInteger.valueOf(Long.valueOf(attrs[1])),
                    attrs[2], attrs[3], attrs[4], BigInteger.valueOf(Long.valueOf(attrs[5])),
                    BigInteger.valueOf(Long.valueOf(attrs[6])),
                    Double.valueOf(attrs[7]), Double.valueOf(attrs[8]));
            if(task.endTime.compareTo(task.startTime) < 0){
                task.endTime = task.startTime;
            }
            Job job = res.getOrDefault(task.jobName, new Job(task.jobName));
            job.addTask(task);
            res.put(task.jobName, job);
            curNum++;
            if(curNum % 1000 == 0){
                System.out.println(curNum + " / " + 14295731);
            }
            if(curNum > iteration){
                break;
            }
        }
        br.close();
        return res;
    }

    public static void writeFile(List<String> keys, Map<String, Job> map, String fileName) throws Exception {
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
        for (String key : keys){
            Job curJob = map.get(key);
            bw.write(curJob + " " + curJob.tasks.size() + "\n");
            for(Task t : curJob.tasks){
                bw.write(t + "\n");
            }
        }
        bw.close();
    }
}
class Task{
    public String taskName;
    public BigInteger instanceNum;
    public String jobName;
    public String taskType;
    public String taskStatus;
    public BigInteger startTime;
    public BigInteger endTime;
    public double cpuNeed;
    public double memorySize;
    public List<String> parents;
    // List<Task> children;
    public String taskId;//jobName_taskName => taskNum

    public Task(String taskName, BigInteger instanceNum, String jobName, String taskType, String taskStatus, BigInteger startTime, BigInteger endTime, double cpuNeed, double memorySize) {
        this.taskName = taskName;
        this.instanceNum = instanceNum;
        this.jobName = jobName;
        this.taskType = taskType;
        this.taskStatus = taskStatus;
        this.startTime = startTime;
        this.endTime = endTime;
        this.cpuNeed = cpuNeed;
        this.memorySize = memorySize;
        this.parents = new ArrayList<>();
        generateParentsAndId();
        //this.children = new ArrayList<>();
    }
    private void generateParentsAndId(){
        String[] tasks = taskName.split("_");
        //taskId = jobName + "_" + tasks[0].substring(1);
        taskId = tasks[0].substring(1);
        parents.addAll(Arrays.asList(tasks).subList(1, tasks.length));
    }

    @Override
    public String toString() {
        return "Task{" +
                "taskName='" + taskName + '\'' +
                ", instanceNum=" + instanceNum +
                ", jobName='" + jobName + '\'' +
                ", taskType='" + taskType + '\'' +
                ", taskStatus='" + taskStatus + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", cpuNeed=" + cpuNeed +
                ", memorySize=" + memorySize +
                ", parents=" + parents +
                ", taskId='" + taskId + '\'' +
                '}';
    }
}
class Job{
    public String jobName;
    public BigInteger startTime = BigInteger.valueOf(Integer.MAX_VALUE);
    public BigInteger endTime = BigInteger.valueOf(-1);
    public List<Task> tasks = new ArrayList<>();

    public Job(String jobName) {
        this.jobName = jobName;
    }

    public void addTask(Task t){
        tasks.add(t);
        startTime = startTime.min(t.startTime);
        endTime = endTime.max(t.endTime);
    }

    @Override
    public String toString() {
        return "Job{" +
                "jobName='" + jobName + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}

class GNode{
    public int outDegree;
    public int inDegree;
    public String id;

    public GNode(int outDegree, int inDegree, String id) {
        this.outDegree = outDegree;
        this.inDegree = inDegree;
        this.id = id;
    }
}