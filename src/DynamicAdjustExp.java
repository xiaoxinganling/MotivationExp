import java.io.BufferedWriter;
import java.io.FileWriter;
import java.math.BigInteger;
import java.util.*;

public class DynamicAdjustExp {
    private static double MEMORY_SIZE = 0.7;
    public static void main(String[] args) throws Exception{
        Map<String, Job> res = SketchExp.singleJobOutDegreeExp("C:\\Users\\xiaoxinganling\\Desktop\\batch_task.csv");
        List<String> keys = new ArrayList<>(res.keySet());
        Collections.sort(keys, (o1, o2) -> res.get(o1).startTime.subtract(res.get(o2).startTime).intValue());
        for(String k : keys){
            if(res.get(k).tasks.size() == 33){
                Job j = res.get(k);
                Map<String, Task> tasks = new HashMap<>();
                for(Task t : j.tasks){
                    tasks.put(t.taskId, t);
                }
                System.out.println(j.jobName);
                // get graph
                Map<String, GNode> graph = SketchExp.getGraph(j);
                // get endTask
                String endTask = SketchExp.getEndTask(graph);
                BufferedWriter bw = new BufferedWriter(new FileWriter("dynamic_res_one_job"));
                // 随着出边数的增加，cache收益的变化
                List<Integer> decreaseTime = new ArrayList<>();
                for(int i = 1; i < 12; i++){
                    Set<String> cache = SketchExp.getTasksWithOutdegree(j, i);
                    decreaseTime.add(getJCT(j, endTask).subtract(getJCTWithCache(j, cache, endTask)).intValue());
                }
                bw.write(decreaseTime + "\n");
                System.out.println(decreaseTime);
                // 随着距离action远近的增加，cache收益的变化
                decreaseTime = new ArrayList<>();
                for(int i = 1; i < 12; i++){
                    Set<String> cache = SketchExp.getTasksWithStep(j, i, endTask);
                    decreaseTime.add(getJCT(j, endTask).subtract(getJCTWithCache(j, cache, endTask)).intValue());
                }
                System.out.println(decreaseTime);
                bw.write(decreaseTime + "\n");
                // print tasks' waiting time
                getTaskWaitTime(j, tasks);
                // 测试memorysize选定的情况下，选择的task
//                System.out.println(getTasksWithOutdegreeAndMemory(j,1,MEMORY_SIZE));
                System.out.println(getTasksWithStepAndMemory(j, 2, endTask, MEMORY_SIZE));
                // 给定memory size情况下
                // 随着出边数增加，cache收益的变化
                List<Double> memorySize = new ArrayList<>();
                decreaseTime = new ArrayList<>();
                for(int i = 1; i < 12; i++){
                    Set<String> cache = getTasksWithOutdegreeAndMemory(j, i, MEMORY_SIZE);
                    //decreaseTime.add(SketchExp.getTimeWithCache(j, cache, endTask).intValue());
                    decreaseTime.add(getJCT(j, endTask).subtract(getJCTWithCache(j, cache, endTask)).intValue());
                    memorySize.add(SketchExp.getSizeWithCache(j, cache));
                }
                bw.write(decreaseTime + "\n");
                bw.write(memorySize + "\n");
                System.out.println(decreaseTime);
                System.out.println(memorySize);
                // 随着距离endTask跳数的增加，cache收益的变化
                decreaseTime = new ArrayList<>();
                memorySize = new ArrayList<>();
                for(int i = 1; i < 12; i++){
                    Set<String> cache = getTasksWithStepAndMemory(j, i, endTask, MEMORY_SIZE);
                    //decreaseTime.add(SketchExp.getTimeWithCache(j, cache, endTask).intValue());
                    decreaseTime.add(getJCT(j, endTask).subtract(getJCTWithCache(j, cache, endTask)).intValue());
                    memorySize.add(SketchExp.getSizeWithCache(j, cache));
                }
                System.out.println(decreaseTime);
                System.out.println(memorySize);
                bw.write(decreaseTime + "\n");
                bw.write(memorySize + "\n");
                bw.close();
                return;
            }
        }
    }
    // 1. 根据job执行图计算时间
    public static BigInteger getJCT(Job j, String endTask){
//        // 算法1. 直接根据endtime-starttime,事实证明的确不行
//        System.out.println("total time " + j.endTime.subtract(j.startTime));
//        return j.endTime.subtract(j.startTime);
        BigInteger totalTime = BigInteger.valueOf(0);
        for(Task t : j.tasks){
            if(t.taskId.equals(endTask)){
                Map<String, Task> tasks = new HashMap<>();
                for(Task tt : j.tasks){
                    tasks.put(tt.taskId, tt);
                }
                totalTime = dfs(tasks, t);
                break;
            }
        }
        return totalTime;
    }
    public static BigInteger dfs(Map<String, Task> tasks, Task t){
        BigInteger res = t.endTime.subtract(t.startTime);
        BigInteger max = BigInteger.valueOf(0);
        for(String next : t.parents){
            Task cur = tasks.get(next);
            max = max.max(dfs(tasks, cur));
        }
        return res.add(max);
    }
    // 2. 根据job和cache结果计算时间
    public static BigInteger getJCTWithCache(Job j, Set<String> cache, String endTask){
        Map<String, Task> tasks = new HashMap<>();
        for(Task tt : j.tasks){
            tasks.put(tt.taskId, tt);
        }
        Set<String> needToCalculate = new HashSet<>();
        needToCalculate.add(endTask);
        Queue<Task> queue = new LinkedList<>();
        queue.offer(tasks.get(endTask));
        while(!queue.isEmpty()){
            Task cur = queue.poll();
            for(String parent : cur.parents) {
                if (cache.contains(parent)) {
                    continue;
                }
                queue.offer(tasks.get(parent));
                needToCalculate.add(parent);
            }
        }
        System.out.println("cache" + cache);
        System.out.println("need to caculate " + needToCalculate);
        BigInteger totalTime = dfsWithCache(tasks, tasks.get(endTask), needToCalculate);
        System.out.println(totalTime);
        return totalTime;
    }
    public static BigInteger dfsWithCache(Map<String, Task> tasks, Task t, Set<String> needToCalculate) {
        BigInteger res = t.endTime.subtract(t.startTime);
        BigInteger max = BigInteger.valueOf(0);
        for (String next : t.parents) {
            if (needToCalculate.contains(next)) {
                Task cur = tasks.get(next);
                max = max.max(dfsWithCache(tasks, cur, needToCalculate));//这里写错了
            }

        }
        return res.add(max);
    }
    // 3. 计算job中各个task的等待时间
    public static void getTaskWaitTime(Job j, Map<String, Task> tasks){
        for(Task t : j.tasks){
            if(t.parents.size() == 0){
                System.out.println(t + " 0");
            }else{
                BigInteger latest = BigInteger.valueOf(0);
                for(String parent : t.parents){
                    latest = latest.max(tasks.get(parent).endTime);
                }
                System.out.println(t + " \n" + t.startTime.subtract(latest));//maybe minus
            }
        }
    }
    // 4. 根据memory size选取Outdegree为某个值的task
    public static Set<String> getTasksWithOutdegreeAndMemory(Job j, int outDegree, double memorySize){
        Set<String> res = new HashSet<>();
        Map<String, GNode> graph = SketchExp.getGraph(j);
        Map<String, Task> tasks = new HashMap<>();
        for(Task t : j.tasks){
            tasks.put(t.taskId, t);
        }
        //按照开始时间的降序排个序
        List<String> sortedKeyset = new ArrayList<>(graph.keySet());
        Collections.sort(sortedKeyset, (o1, o2) -> tasks.get(o2).startTime.subtract(tasks.get(o1).startTime).intValue());
//        for(String key : sortedKeyset){
//            System.out.println(tasks.get(key).startTime + " " + key);
//        }// startTime其实是一样的
        for(String key : sortedKeyset){
            if(graph.get(key).outDegree == outDegree){
                if(memorySize - tasks.get(key).memorySize < 0){
                    break;
                }
                res.add(key);
                //System.out.println(key + " " + tasks.get(key).memorySize);
                memorySize -= tasks.get(key).memorySize;
            }
        }
        return res;
    }

    // 3. get tasks with step and Memory size
    public static Set<String> getTasksWithStepAndMemory(Job j, int step, String endTask, double memory){
        Map<String, Task> tasks = new HashMap<>();
        for(Task t : j.tasks){
            tasks.put(t.taskId, t);
        }
        Queue<Task> queue = new LinkedList<>();
        queue.offer(tasks.get(endTask));
        while(step > 0 && !queue.isEmpty()){
            int size = queue.size();
            for(int i = 0; i < size; i++){
                Task cur = queue.poll();
                for(String parent : cur.parents){
                    queue.offer(tasks.get(parent));
                }
            }
            step--;
        }
        Set<String> res = new HashSet<>();
        while(!queue.isEmpty()){
            Task toBeAdd = queue.poll();
            if(memory - toBeAdd.memorySize < 0){
                break;
            }
            res.add(toBeAdd.taskId);
            memory -= toBeAdd.memorySize;
            System.out.println("add " + toBeAdd.taskId + " " +memory);
        }
        return res;
    }
}
