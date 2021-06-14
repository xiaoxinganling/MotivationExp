package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

public class GenerateGraph {
    public static void main(String[] args) throws Exception {
        String tasks = "Task{taskName='R2_1', instanceNum=5, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.49, parents=[1], taskId='j_45713_2'}\n" +
                "Task{taskName='R18_28', instanceNum=23, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86441, cpuNeed=100.0, memorySize=0.49, parents=[28], taskId='j_45713_18'}\n" +
                "Task{taskName='J3_2_22', instanceNum=7, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.59, parents=[2, 22], taskId='j_45713_3'}\n" +
                "Task{taskName='R19_18', instanceNum=23, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86441, cpuNeed=100.0, memorySize=0.49, parents=[18], taskId='j_45713_19'}\n" +
                "Task{taskName='M16', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86436, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_45713_16'}\n" +
                "Task{taskName='R10_9', instanceNum=3, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86438, cpuNeed=100.0, memorySize=0.49, parents=[9], taskId='j_45713_10'}\n" +
                "Task{taskName='R30_29', instanceNum=23, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.49, parents=[29], taskId='j_45713_30'}\n" +
                "Task{taskName='J28_11_17_26_27', instanceNum=23, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.59, parents=[11, 17, 26, 27], taskId='j_45713_28'}\n" +
                "Task{taskName='R6_5', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86435, cpuNeed=100.0, memorySize=0.39, parents=[5], taskId='j_45713_6'}\n" +
                "Task{taskName='M21', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86434, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_45713_21'}\n" +
                "Task{taskName='R13_12', instanceNum=23, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.49, parents=[12], taskId='j_45713_13'}\n" +
                "Task{taskName='J20_19_22', instanceNum=25, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86441, cpuNeed=100.0, memorySize=0.59, parents=[19, 22], taskId='j_45713_20'}\n" +
                "Task{taskName='R29_28', instanceNum=23, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.49, parents=[28], taskId='j_45713_29'}\n" +
                "Task{taskName='M23', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86435, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_45713_23'}\n" +
                "Task{taskName='R4_26', instanceNum=5, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86442, cpuNeed=100.0, memorySize=0.49, parents=[26], taskId='j_45713_4'}\n" +
                "Task{taskName='J17_15_16', instanceNum=7, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86441, cpuNeed=100.0, memorySize=0.59, parents=[15, 16], taskId='j_45713_17'}\n" +
                "Task{taskName='R12_28', instanceNum=23, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.49, parents=[28], taskId='j_45713_12'}\n" +
                "Task{taskName='R25_24', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86435, cpuNeed=100.0, memorySize=0.39, parents=[24], taskId='j_45713_25'}\n" +
                "Task{taskName='R24_23', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86435, cpuNeed=100.0, memorySize=0.49, parents=[23], taskId='j_45713_24'}\n" +
                "Task{taskName='R32_3_14_20_31', instanceNum=83, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.49, parents=[3, 14, 20, 31], taskId='j_45713_32'}\n" +
                "Task{taskName='J31_22_30', instanceNum=25, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.59, parents=[22, 30], taskId='j_45713_31'}\n" +
                "Task{taskName='R1_28', instanceNum=9, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.49, parents=[28], taskId='j_45713_1'}\n" +
                "Task{taskName='J11_4_10', instanceNum=9, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.59, parents=[4, 10], taskId='j_45713_11'}\n" +
                "Task{taskName='R15_26', instanceNum=5, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86441, cpuNeed=100.0, memorySize=0.49, parents=[26], taskId='j_45713_15'}\n" +
                "Task{taskName='M5', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86435, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_45713_5'}\n" +
                "Task{taskName='R22_21', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86435, cpuNeed=100.0, memorySize=0.39, parents=[21], taskId='j_45713_22'}\n" +
                "Task{taskName='M26_25', instanceNum=5, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86441, cpuNeed=100.0, memorySize=0.3, parents=[25], taskId='j_45713_26'}\n" +
                "Task{taskName='M9_6_8', instanceNum=3, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86438, cpuNeed=100.0, memorySize=0.49, parents=[6, 8], taskId='j_45713_9'}\n" +
                "Task{taskName='R8_7', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86435, cpuNeed=100.0, memorySize=0.39, parents=[7], taskId='j_45713_8'}\n" +
                "Task{taskName='M27', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86435, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_45713_27'}\n" +
                "Task{taskName='M7', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86435, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_45713_7'}\n" +
                "Task{taskName='R33_32', instanceNum=1, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.39, parents=[32], taskId='j_45713_33'}\n" +
                "Task{taskName='J14_13_22', instanceNum=25, jobName='j_45713', taskType='1', taskStatus='Terminated', startTime=86434, endTime=86443, cpuNeed=100.0, memorySize=0.59, parents=[13, 22], taskId='j_45713_14'}\n";
            String tasks2 = "Task{taskName='M4', instanceNum=1, jobName='j_169363', taskType='1', taskStatus='Terminated', startTime=85067, endTime=85072, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_169363_4'}\n" +
                    "Task{taskName='J10_1_7_9', instanceNum=22, jobName='j_169363', taskType='1', taskStatus='Terminated', startTime=85157, endTime=86404, cpuNeed=100.0, memorySize=0.49, parents=[1, 7, 9], taskId='j_169363_10'}\n" +
                    "Task{taskName='M6', instanceNum=1, jobName='j_169363', taskType='1', taskStatus='Terminated', startTime=85067, endTime=85100, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_169363_6'}\n" +
                    "Task{taskName='M2', instanceNum=187, jobName='j_169363', taskType='1', taskStatus='Terminated', startTime=85067, endTime=85099, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_169363_2'}\n" +
                    "Task{taskName='M1', instanceNum=343, jobName='j_169363', taskType='1', taskStatus='Terminated', startTime=85067, endTime=85076, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_169363_1'}\n" +
                    "Task{taskName='M3', instanceNum=4, jobName='j_169363', taskType='1', taskStatus='Terminated', startTime=85067, endTime=85089, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_169363_3'}\n" +
                    "Task{taskName='J8_6_7', instanceNum=3, jobName='j_169363', taskType='1', taskStatus='Terminated', startTime=85101, endTime=85122, cpuNeed=100.0, memorySize=0.59, parents=[6, 7], taskId='j_169363_8'}\n" +
                    "Task{taskName='M5', instanceNum=101, jobName='j_169363', taskType='1', taskStatus='Terminated', startTime=85067, endTime=85097, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_169363_5'}\n" +
                    "Task{taskName='J9_2_3_4_5_8', instanceNum=149, jobName='j_169363', taskType='1', taskStatus='Terminated', startTime=85123, endTime=85156, cpuNeed=100.0, memorySize=0.79, parents=[2, 3, 4, 5, 8], taskId='j_169363_9'}\n" +
                    "Task{taskName='M7', instanceNum=1, jobName='j_169363', taskType='1', taskStatus='Terminated', startTime=85067, endTime=85092, cpuNeed=100.0, memorySize=0.3, parents=[], taskId='j_169363_7'}\n";
            printGraph(tasks);
    }
    private static void printGraph(String tasks) throws IOException {
        BufferedReader br = new BufferedReader(new StringReader(tasks));
        String line;
        while((line = br.readLine())!=null){
            int start = line.indexOf('\'');
            int end = line.substring(start + 1).indexOf('\'');
            String taskName = line.substring(start + 1, start + end + 1);
            String[] elements = taskName.split("_");
            for(int i = 1; i < elements.length; i++){
                System.out.println(elements[i] + "-->" + elements[0].substring(1) + ";");
            }
        }
        br.close();
    }
}
