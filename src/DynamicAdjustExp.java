import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class DynamicAdjustExp {
    private static final double MEMORY_SIZE = 0.7;
    private static int TOTAl = 0;
    public static void main(String[] args) throws Exception{
        singleJobExp();
    }

    // 单job dynamic adjust exp
    public static void singleJobExp() throws Exception{
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
                // 测试全排列
//                System.out.println(getTaskListWithStepAndMemory(j, 3, endTask, MEMORY_SIZE).keySet().size());
//                System.out.println(getTaskListWithStepAndMemory(j, 13, endTask, MEMORY_SIZE).keySet().size());
                bw.close();
                List<Double> avgDecreaseTime = new ArrayList<>();
                memorySize = new ArrayList<>();
                chooseCondition(1, 12, avgDecreaseTime, memorySize,j, endTask);
                // test all conditions
//                StringBuilder sb = new StringBuilder();
//                int mul = 1;
//                for(int i = 1; i < 12; i++){
//                    int size = getTaskListWithStepAndMemory(j, i, endTask, MEMORY_SIZE).keySet().size();
//                    System.out.println("step: " + i + ", conditions: " + size);
//                    sb.append(size).append("x");
//                    mul *= size;
//                }
//                sb.deleteCharAt(sb.length() - 1);
//                System.out.println(sb.toString() + " multiply: "+ mul);
                //chooseCondition(1, 12, decreaseTime, memorySize, j, endTask);
//                Set<String> cache1 = new HashSet<>();
//                cache1.add("20");
//                Set<String> cache2 = new HashSet<>();
//                cache2.add("31");
//                Set<String> cache3 = new HashSet<>();
//                cache3.add("3");
//                Set<String> cache4 = new HashSet<>();
//                cache4.add("14");
//                System.out.println(cache1);
//                System.out.println(getJCT(j, endTask) + " ============== " + getJCT(j, endTask).subtract(getJCTWithCache(j, cache1, endTask)).intValue());
//                System.out.println(cache2);
//                System.out.println(getJCT(j, endTask).subtract(getJCTWithCache(j, cache2, endTask)).intValue());
//                System.out.println(cache3);
//                System.out.println(getJCT(j, endTask).subtract(getJCTWithCache(j, cache3, endTask)).intValue());
//                System.out.println(cache4);
//                System.out.println(getJCT(j, endTask).subtract(getJCTWithCache(j, cache4, endTask)).intValue());
                return;
            }
        }
    }
    // 1. 根据job执行图计算时间
    public static BigInteger getJCT(Job j, String endTask){
//        // 算法1. 直接根据endtime-starttime,事实证明的确不行
//        System.out.println("total time " + j.endTime.subtract(j.startTime));
//        return j.endTime.subtract(j.startTime);
        Map<String, Task> tasks = new HashMap<>();
        for(Task t : j.tasks){
            tasks.put(t.taskId, t);
        }
        return dfs(tasks, tasks.get(endTask));
    }
    public static BigInteger dfs(Map<String, Task> tasks, Task t){
        BigInteger res = t.endTime.subtract(t.startTime);
        BigInteger max = BigInteger.valueOf(0);
        for(String next : t.parents){
            max = max.max(dfs(tasks, tasks.get(next)));
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
        System.out.println(needToCalculate + " " + needToCalculate.size());
//        System.out.println("cache" + cache);
//        System.out.println("need to caculate " + needToCalculate);
        BigInteger totalTime = dfsWithCache(tasks, tasks.get(endTask), needToCalculate);
        System.out.println("total Time With Cache: " + totalTime);
        return totalTime;
    }
    public static BigInteger dfsWithCache(Map<String, Task> tasks, Task t, Set<String> needToCalculate) {
        BigInteger res = t.endTime.subtract(t.startTime);
        BigInteger max = BigInteger.valueOf(0);
        for (String next : t.parents) {
            if (needToCalculate.contains(next)) {
                Task cur = tasks.get(next);
                BigInteger tmp = dfsWithCache(tasks, cur, needToCalculate);
//                if(t.taskId.equals("32")){
//                    System.out.println("chosed " + next);
//                    System.out.println("tmp.value? " + tmp + " tmp is bigger ? " + tmp.compareTo(max));
//                }//我代码是对的，没毛病
                max = max.max(tmp);//这里写错了
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
        getTasksWithStepByQueue(step, tasks, queue);
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

    // 4. get tasks with step and memory size different sequence
    public static Map<String, Set<String>> getTaskListWithStepAndMemory(Job j, int step, String endTask, double memory){
        Map<String, Task> tasks = new HashMap<>();
        for(Task t : j.tasks){
            tasks.put(t.taskId, t);
        }
        Queue<Task> queue = new LinkedList<>();
        queue.offer(tasks.get(endTask));
        getTasksWithStepByQueue(step, tasks, queue);
        // 目前所有的item都在queue里面
        List<Task> items = new ArrayList<>();
        while(!queue.isEmpty()){
            Task cur = queue.poll();
            if(!items.contains(cur)){
                items.add(cur);//去个重,错把cur写成queue.poll()
            }
        }
        // 全排列
        List<List<Task>> allCondition = new ArrayList<>();
        backtrack(0, items, allCondition);
        System.out.println(items.size() + " items generates " + allCondition.size() + " conditions with memory size: " + MEMORY_SIZE);
        // 返回结果
        Map<String, Set<String>> res = new HashMap<>();
        for(List<Task> condition : allCondition){
            // 不能在这排序，不然全排列就没意义了
            Set<String> oneCache = new HashSet<>();
            if(condition.size() == 0){
                res.put("null", oneCache);
            }
            //重置memory
            memory = MEMORY_SIZE;
            List<String> chosed = new ArrayList<>();
            for(Task toBeAdd : condition){
                if(memory - toBeAdd.memorySize < 0){//这里有个bug,如果memory_size是0.7，只有一个task是0.5，那么它就不会被添加到condition里面
                    //需要break
                    break;
                }
                oneCache.add(toBeAdd.taskId);
                memory -= toBeAdd.memorySize;
                //key.append(toBeAdd.taskId).append("_");
                chosed.add(toBeAdd.taskId);
            }
            // put into map
            Collections.sort(chosed);
            StringBuilder key = new StringBuilder();
            for(String s : chosed){
                key.append(s).append("_");
            }
            //System.out.println(key.toString() + res.containsKey(key.toString()));
            if(!res.containsKey(key.toString())){//其实这里不能去重因为2_22和22_2分不出来
//                        double sum = 0;
//                        for(String s : chosed){
//                            sum += tasks.get(s).memorySize;
//                            System.out.print(tasks.get(s).memorySize + " ");
//                        }
//                        System.out.println(sum);
                res.put(key.toString(), oneCache);
            }
        }
        System.out.printf("===========> after filter: %d conditions\n", res.keySet().size());
        return res;
    }

    // 5. 将不同step得到的cache结果进行组合
    private static void chooseCondition(int curStep, int totalStep, List<Double> decreaseTime,
                                        List<Double> memorySize, Job j, String endTask) throws Exception {
        //[curStep, totalStep]
        if(curStep == totalStep){
            BufferedWriter bw = new BufferedWriter(new FileWriter("dynamic_res_sequence_average_one_job"));
            bw.write(decreaseTime + "\n");
            bw.write(memorySize + "\n");
            bw.close();
            return;
        }
        Map<String, Set<String>> cacheCondition = getTaskListWithStepAndMemory(j, curStep, endTask, MEMORY_SIZE);
        System.out.println("step: " + curStep + " generates " + cacheCondition.size() + " conditions");
        System.out.println(cacheCondition.values());
        // get average
        double timeSum = 0;
        double sizeSum = 0;
        for(Set<String> cache : cacheCondition.values()){
            timeSum += getJCT(j, endTask).subtract(getJCTWithCache(j, cache, endTask)).intValue();
            sizeSum += SketchExp.getSizeWithCache(j, cache);
        }
        decreaseTime.add(timeSum / cacheCondition.size());
        memorySize.add(sizeSum / cacheCondition.size());
        chooseCondition(curStep + 1, totalStep, decreaseTime, memorySize, j, endTask);

//        for(Set<String> cache : cacheCondition.values()){
//            decreaseTime.add(getJCT(j, endTask).subtract(getJCTWithCache(j, cache, endTask)).intValue());
//            memorySize.add(SketchExp.getSizeWithCache(j, cache));
//            System.out.println(cache + " " + decreaseTime + " " + memorySize);
//            chooseCondition(curStep + 1, totalStep, decreaseTime, memorySize, j, endTask);
//            decreaseTime.remove(decreaseTime.size() - 1);
//            memorySize.remove(memorySize.size() - 1);
//        }
    }
    private static void getTasksWithStepByQueue(int step, Map<String, Task> tasks, Queue<Task> queue) {
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
    }

    // 对item进行全排列
    private static void backtrack(int depth, List<Task> items, List<List<Task>> allCondition){
        if(depth == items.size()){
            allCondition.add(new ArrayList<>(items));
//            for(Task t : items){
//                System.out.print(t.taskId + " ");
//            }
//            System.out.println();
            return;//记得return
        }
        for(int i = depth; i <  items.size(); i++){
            swap(items, i, depth);
            backtrack(depth + 1, items, allCondition);
            swap(items, i, depth);
        }
    }
    private static void swap(List<Task> items, int i, int j){
        if(i == j){
            return;
        }
        Task tmp = items.get(i);
        items.set(i, items.get(j));
        items.set(j, tmp);
    }
    // 原有流程: 根据出度选择一个内存集合，根据内存大小进行筛选得到cache set；把cache set拿过去算时间，算内存占用
    // 现在流程: 根据出度选择一个内存集合，*引入一个新的顺序*，根据内存大小进行筛选得到cache set，（~后面一致）
    // 新的顺序：就是全排列，然后每个step的全排列进行进行组合；原来是一个cache set，现在是一个cache set的list（或者map）
}
