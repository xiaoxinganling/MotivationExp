import java.io.BufferedWriter;
import java.io.FileWriter;
import java.math.BigInteger;
import java.util.*;

public class DynamicAdjustExp {
    public static void main(String[] args) throws Exception{
        Map<String, Job> res = SketchExp.singleJobOutDegreeExp("C:\\Users\\xiaoxinganling\\Desktop\\batch_task.csv");
        List<String> keys = new ArrayList<>(res.keySet());
        Collections.sort(keys, (o1, o2) -> res.get(o1).startTime.subtract(res.get(o2).startTime).intValue());
        for(String k : keys){
            if(res.get(k).tasks.size() == 33){
                Job j = res.get(k);
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
    public static BigInteger dfsWithCache(Map<String, Task> tasks, Task t, Set<String> needToCalculate){
        BigInteger res = t.endTime.subtract(t.startTime);
        BigInteger max = BigInteger.valueOf(0);
        for(String next : t.parents){
            if(needToCalculate.contains(next)){
                Task cur = tasks.get(next);
                max = max.max(dfsWithCache(tasks, cur, needToCalculate));//这里写错了
            }

        }
        return res.add(max);
    }
}
