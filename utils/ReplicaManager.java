package utils;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaManager {

    private final Map<Integer, String> storageServers; // 存储机编号 -> 地址
    private final Map<String, List<Integer>> fileReplicaMap; // 文件名 -> 存储机编号列表
    private final Set<Integer> aliveServers; // 当前存活的存储机编号集合
    private final Set<Integer> previousAliveServers; // 上次检测的存活存储机编号集合
    private final Set<String> queriedFiles; // 已成功查询的文件集合
    private final Map<String, Set<Integer>> queriedReplicas; // 已查询的副本记录 (文件名 -> 已查询副本)

    public ReplicaManager(Map<Integer, String> storageServers, Map<String, List<Integer>> fileReplicaMap) {
        this.storageServers = storageServers;
        this.fileReplicaMap = fileReplicaMap;
        this.aliveServers = ConcurrentHashMap.newKeySet();
        this.previousAliveServers = ConcurrentHashMap.newKeySet();
        this.queriedFiles = ConcurrentHashMap.newKeySet();
        this.queriedReplicas = new ConcurrentHashMap<>();

        // 初始化为所有存储机存活状态
        this.aliveServers.addAll(storageServers.keySet());
        this.previousAliveServers.addAll(storageServers.keySet());
    }

    /**
     * 获取主块所在的存储机编号。
     *
     * @param fileName 文件名
     * @return 主块存储机编号，或 -1 如果主块不可用。
     */
    public synchronized int getPrimaryServer(String fileName) {
        List<Integer> replicas = fileReplicaMap.get(fileName);
        if (replicas != null && !replicas.isEmpty()) {
            int primaryServer = replicas.get(0); // 主块为第一个编号
            if (aliveServers.contains(primaryServer)) {
                return primaryServer;
            }
        }
        return -1; // 主块不可用
    }

    /**
     * 获取一个可用的副本存储机编号。
     *
     * @param fileName 文件名
     * @return 可用副本存储机编号，或 -1 如果没有可用副本。
     */
    public synchronized int getAvailableReplica(String fileName) {
        List<Integer> replicas = fileReplicaMap.get(fileName);
        if (replicas == null || replicas.size() <= 1) {
            return -1; // 没有副本可用
        }

        Set<Integer> alreadyQueried = queriedReplicas.getOrDefault(fileName, ConcurrentHashMap.newKeySet());
        for (int i = 1; i < replicas.size(); i++) { // 跳过主块，从副本中寻找
            int replicaServer = replicas.get(i);
            if (aliveServers.contains(replicaServer) && !alreadyQueried.contains(replicaServer)) {
                alreadyQueried.add(replicaServer); // 标记为已查询
                queriedReplicas.put(fileName, alreadyQueried);
                return replicaServer;
            }
        }
        return -1; // 无可用副本
    }

    /**
     * 标记文件的主块或副本查询成功。
     *
     * @param fileName 文件名
     * @return 是否标记成功（若文件已被标记，返回 false）。
     */
    public synchronized boolean markFileAsQueried(String fileName) {
        if (queriedFiles.contains(fileName)) {
            return false; // 文件已查询，跳过
        }
        queriedFiles.add(fileName);
        return true;
    }

    /**
     * 检测所有存储机的状态并通知用户状态变化。
     */
    public synchronized void checkServerHealth() {
        Set<Integer> currentAliveServers = ConcurrentHashMap.newKeySet();

        for (Integer serverId : storageServers.keySet()) {
            String address = storageServers.get(serverId);
            String[] parts = address.split(":");
            String ip = parts[0];
            int port = Integer.parseInt(parts[1]);

            try (Socket socket = new Socket(ip, port)) {
                currentAliveServers.add(serverId); // 如果连接成功，则存活
            } catch (IOException e) {
                // 如果连接失败，则认为宕机
            }
        }

        // 动态比较新状态和旧状态
        for (Integer serverId : currentAliveServers) {
            if (!previousAliveServers.contains(serverId)) {
                System.out.println("服务器上线: " + storageServers.get(serverId));
            }
        }

        for (Integer serverId : previousAliveServers) {
            if (!currentAliveServers.contains(serverId)) {
                System.out.println("服务器宕机: " + storageServers.get(serverId));
            }
        }

        // 更新存活服务器状态
        aliveServers.clear();
        aliveServers.addAll(currentAliveServers);
        previousAliveServers.clear();
        previousAliveServers.addAll(currentAliveServers);
    }

    /**
     * 获取存活的存储机列表。
     *
     * @return 存活的存储机编号集合
     */
    public synchronized Set<Integer> getAliveServers() {
        return new HashSet<>(aliveServers);
    }

    /**
     * 获取当前存储机状态快照。
     *
     * @return 存储机状态快照 (包含存活与宕机列表)
     */
    public synchronized Map<String, List<Integer>> getServerStatusSnapshot() {
        Map<String, List<Integer>> status = new HashMap<>();
        List<Integer> alive = new ArrayList<>();
        List<Integer> down = new ArrayList<>();

        for (Integer serverId : storageServers.keySet()) {
            if (aliveServers.contains(serverId)) {
                alive.add(serverId);
            } else {
                down.add(serverId);
            }
        }

        status.put("alive", alive);
        status.put("down", down);
        return status;
    }

    /**
     * 根据存储机编号获取服务器地址。
     *
     * @param serverId 存储机编号。
     * @return 存储机地址 (IP:PORT)，如果不存在则返回 null。
     */
    public synchronized String getServerAddress(int serverId) {
        return storageServers.getOrDefault(serverId, null);
    }
}