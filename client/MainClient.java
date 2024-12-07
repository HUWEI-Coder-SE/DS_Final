package client;

import utils.ReplicaManager;

import java.util.*;
import java.util.concurrent.*;

/**
 * MainClient 类，负责引导用户输入查询条件并向存储虚拟机发送查询请求。
 */
public class MainClient {

    public static void main(String[] args) {
        // 配置文件副本信息
        Map<String, List<Integer>> fileReplicaMap = new HashMap<>();
        fileReplicaMap.put("chunk_1.lson", Arrays.asList(1, 2, 3));
        fileReplicaMap.put("chunk_2.lson", Arrays.asList(2, 3, 4));
        fileReplicaMap.put("chunk_3.lson", Arrays.asList(3, 4, 1));
        fileReplicaMap.put("chunk_4.lson", Arrays.asList(4, 1, 2));

        // 配置存储机地址
        Map<Integer, String> storageServers = new HashMap<>();
        storageServers.put(1, "127.0.0.1:8081");
        storageServers.put(2, "127.0.0.1:8082");
        storageServers.put(3, "127.0.0.1:8083");
        storageServers.put(4, "127.0.0.1:8084");

        // 创建副本管理器
        ReplicaManager replicaManager = new ReplicaManager(storageServers, fileReplicaMap);

        // 打印存储机的当前状态
        System.out.println("正在初始化存储机状态...");
        replicaManager.checkServerHealth();
        Map<String, List<Integer>> serverStatus = replicaManager.getServerStatusSnapshot();
        System.out.println("当前存储机状态：");
        System.out.println("存活的存储机: " + serverStatus.get("alive"));
        System.out.println("宕机的存储机: " + serverStatus.get("down"));

        // 用户输入引导
        Scanner scanner = new Scanner(System.in);
        System.out.print("请输入查询的作者名: ");
        String author = scanner.nextLine().trim();
        System.out.print("请输入开始年份 (-1 表示不限): ");
        int startYear = scanner.nextInt();
        System.out.print("请输入结束年份 (-1 表示不限): ");
        int endYear = scanner.nextInt();

        // 记录查询时间
        long startTime = System.currentTimeMillis();

        // 启动查询任务
        ExecutorService executor = Executors.newFixedThreadPool(fileReplicaMap.size());
        List<Future<Map<Integer, Integer>>> futures = new ArrayList<>();

        try {
            for (String fileName : fileReplicaMap.keySet()) {
                futures.add(executor.submit(new RequestThread(replicaManager, fileName, author)));
            }

            Map<Integer, Integer> aggregatedResults = new HashMap<>();
            for (Future<Map<Integer, Integer>> future : futures) {
                Map<Integer, Integer> result = future.get();
                if (result != null) {
                    for (Map.Entry<Integer, Integer> entry : result.entrySet()) {
                        aggregatedResults.merge(entry.getKey(), entry.getValue(), Integer::sum);
                    }
                }
            }

            // 展示查询结果
            displayResults(author, startYear, endYear, aggregatedResults);

        } catch (InterruptedException | ExecutionException e) {
            System.err.println("查询过程中发生错误: " + e.getMessage());
        } finally {
            executor.shutdown();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("查询耗时: " + (endTime - startTime) + " 毫秒");
    }

    /**
     * 根据时间范围展示查询结果
     */
    private static void displayResults(String author, int startYear, int endYear, Map<Integer, Integer> results) {
        if (results.isEmpty()) {
            System.out.println("未找到该作者 " + author + " 的任何数据。");
            return;
        }

        int totalPapers = 0;
        for (Map.Entry<Integer, Integer> entry : results.entrySet()) {
            int year = entry.getKey();
            int count = entry.getValue();

            if ((startYear == -1 || year >= startYear) && (endYear == -1 || year <= endYear)) {
                totalPapers += count;
            }
        }

        if (startYear == -1 && endYear == -1) {
            System.out.println("作者 " + author + " 的全部论文数量: " + totalPapers);
        } else if (startYear == -1) {
            System.out.println("作者 " + author + " 在 0 到 " + endYear + " 的论文数量: " + totalPapers);
        } else if (endYear == -1) {
            System.out.println("作者 " + author + " 从 " + startYear + " 至今发表的论文数量: " + totalPapers);
        } else {
            System.out.println("作者 " + author + " 在 " + startYear + " 到 " + endYear + " 的论文数量: " + totalPapers);
        }
    }
}