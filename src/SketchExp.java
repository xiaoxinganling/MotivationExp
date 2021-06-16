import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.List;

public class SketchExp {
    private static int maxIteration = 200000;
    public static void main(String[] args) throws Exception {
        Map<String, Job> res = singleJobOutDegreeExp("C:\\Users\\xiaoxinganling\\Desktop\\batch_task.csv");
        List<String> keys = new ArrayList<>(res.keySet());
        Collections.sort(keys, (o1, o2) -> res.get(o1).startTime.subtract(res.get(o2).startTime).intValue());
        for(String k : keys){
            if(res.get(k).tasks.size() == 33){
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
                Job j = res.get(k);
                Map<String, GNode> graph = getGraph(j);
                String endTask = getEndTask(graph);
                BufferedWriter bw = new BufferedWriter(new FileWriter("sketch_res_one_job"));
                // 随着出边数的增加，cache收益和内存占用的变化
                List<Integer> decreaseTime = new ArrayList<>();
                List<Double> memoryConsumption = new ArrayList<>();
                for(int i = 1; i < 12; i++){
                    Set<String> cache = getTasksWithOutdegree(j, i);
                    decreaseTime.add(getTimeWithCache(j,cache,endTask).intValue());
                    memoryConsumption.add(getSizeWithCache(j, cache));
                }
                bw.write(decreaseTime + "\n");
                bw.write(memoryConsumption + "\n");
                System.out.println(decreaseTime);
                System.out.println(memoryConsumption);
                // 随着距离action远近的增加，cache收益和内存占用的变化
                decreaseTime = new ArrayList<>();
                memoryConsumption = new ArrayList<>();
                for(int i = 1; i < 12; i++){
                    Set<String> cache = getTasksWithStep(j,i,endTask);
                    decreaseTime.add(getTimeWithCache(j,cache,endTask).intValue());
                    memoryConsumption.add(getSizeWithCache(j, cache));
                }
                System.out.println(decreaseTime);
                System.out.println(memoryConsumption);
                double averageMem = 0;
                for(double d : memoryConsumption){
                    averageMem += d;
                }
                System.out.println(averageMem / memoryConsumption.size());
                bw.write(decreaseTime + "\n");
                bw.write(memoryConsumption + "\n");
                bw.close();
                return;
            }
        }
        System.out.println(keys.size());
        writeFile(keys, res, "job_description");
    }
    // for every job
    // 1. get execution time with cached tasks
    public static BigInteger getTimeWithCache(Job j, Set<String> cache, String endTask){
        BigInteger before = BigInteger.valueOf(0);
        for(Task t : j.tasks){
            before = before.add(t.endTime.subtract(t.startTime));
        }
        Set<String> needToCalculate = new HashSet<>();
        Queue<Task> queue = new LinkedList<>();
        for(Task t : j.tasks){
            if(t.taskId.equals(endTask)){
                queue.offer(t);
                needToCalculate.add(endTask);
                break;
            }
        }
        while(!queue.isEmpty()){
            Task cur = queue.poll();
            for(String parent : cur.parents) {
                if (cache.contains(parent)) {
                    continue;
                }
                for (Task t : j.tasks) {
                    if (t.taskId.equals(parent)) {
                        queue.offer(t);
                        needToCalculate.add(t.taskId);
                        break;
                    }
                }
            }
        }
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
        Queue<Task> queue = new LinkedList<>();
        for(Task t : j.tasks){
            if(t.taskId.equals(endTask)){
                queue.offer(t);
                break;
            }
        }
        while(step > 0 && !queue.isEmpty()){
            int size = queue.size();
            for(int i = 0; i < size; i++){
                Task cur = queue.poll();
                for(String parent : cur.parents){
                    for(Task t : j.tasks){
                        if(t.taskId.equals(parent)){
                            queue.offer(t);
                            break;
                        }
                    }
                }
            }
            step--;
        }
        Set<String> res = new HashSet<>();
        while(!queue.isEmpty()){
            res.add(queue.poll().taskId);
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

    public static Map<String, Job> singleJobOutDegreeExp(String fileName) throws Exception {
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
            Job job = res.getOrDefault(task.jobName, new Job(task.jobName));
            job.addTask(task);
            res.put(task.jobName, job);
            curNum++;
            if(curNum % 1000 == 0){
                System.out.println(curNum + " / " + 14295731);
            }
            if(curNum > maxIteration){
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