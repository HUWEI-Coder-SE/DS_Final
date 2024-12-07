package server;

import utils.PersistentBTree;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * MainServer 类实现基于索引的多文件请求处理。
 */
public class MainServer {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("用法: java MainServer <端口号> <存储虚拟机文件夹路径>");
            return;
        }

        int port = Integer.parseInt(args[0]); // 服务器端口号
        String serverFolder = args[1]; // 存储虚拟机文件夹路径

        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(5);

        // 加载主块索引
        Map<String, PersistentBTree<String, Long>> mainIndexMap = new HashMap<>();
        Map<String, String> mainDataFileMap = new HashMap<>(); // 主块索引文件 -> 数据文件映射

        // 副本索引动态加载
        Map<String, PersistentBTree<String, Long>> replicaIndexMap = new ConcurrentHashMap<>();
        Map<String, String> replicaDataFileMap = new ConcurrentHashMap<>();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("服务器已启动，正在监听端口: " + port);

            // 遍历文件夹加载主块索引和数据文件
            loadIndexes(serverFolder, mainIndexMap, mainDataFileMap, false);

            if (mainIndexMap.isEmpty()) {
                throw new IllegalStateException("未加载任何主块索引文件，服务器无法启动。");
            }

            System.out.println("成功加载 " + mainIndexMap.size() + " 个主块索引。");

            // 接受客户端连接并处理请求
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("接受到客户端连接: " + clientSocket.getInetAddress());

                // 创建并提交查询线程
                executor.submit(() -> handleRequest(clientSocket, mainIndexMap, mainDataFileMap,
                        replicaIndexMap, replicaDataFileMap, serverFolder));
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("服务器运行时发生错误: " + e.getMessage());
        } finally {
            executor.shutdown(); // 关闭线程池
        }
    }

    /**
     * 处理客户端请求。
     */
    private static void handleRequest(Socket clientSocket,
                                      Map<String, PersistentBTree<String, Long>> mainIndexMap,
                                      Map<String, String> mainDataFileMap,
                                      Map<String, PersistentBTree<String, Long>> replicaIndexMap,
                                      Map<String, String> replicaDataFileMap,
                                      String serverFolder) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String author = in.readLine(); // 接收作者名
            System.out.println("客户端查询: " + author);

            boolean found = false;

            // 优先查找主块
            for (Map.Entry<String, PersistentBTree<String, Long>> entry : mainIndexMap.entrySet()) {
                String indexPath = entry.getKey();
                PersistentBTree<String, Long> bTree = entry.getValue();
                Long pointer = bTree.search(author);

                if (pointer != null) {
                    String dataFilePath = mainDataFileMap.get(indexPath);
                    String authorData = fetchAuthorData(pointer, dataFilePath);

                    if (authorData != null) {
                        out.println(authorData);
                        found = true;
                        break; // 找到数据后立即返回
                    }
                }
            }

            // 如果主块未找到，动态加载并查找副本
            if (!found) {
                synchronized (replicaIndexMap) {
                    if (replicaIndexMap.isEmpty()) {
                        System.out.println("正在加载副本索引...");
                        loadIndexes(serverFolder, replicaIndexMap, replicaDataFileMap, true);
                        System.out.println("副本索引加载完成: " + replicaIndexMap.size() + " 个索引文件。");
                    }
                }

                for (Map.Entry<String, PersistentBTree<String, Long>> entry : replicaIndexMap.entrySet()) {
                    String indexPath = entry.getKey();
                    PersistentBTree<String, Long> bTree = entry.getValue();
                    Long pointer = bTree.search(author);

                    if (pointer != null) {
                        String dataFilePath = replicaDataFileMap.get(indexPath);
                        String authorData = fetchAuthorData(pointer, dataFilePath);

                        if (authorData != null) {
                            out.println(authorData);
                            found = true;
                            break; // 找到数据后立即返回
                        }
                    }
                }
            }

            if (!found) {
                out.println("未找到作者: " + author);
            }

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("处理客户端请求时发生错误: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("关闭客户端连接时发生错误: " + e.getMessage());
            }
        }
    }

    /**
     * 加载索引文件（主块或副本）。
     */
    private static void loadIndexes(String serverFolder,
                                    Map<String, PersistentBTree<String, Long>> indexMap,
                                    Map<String, String> dataFileMap,
                                    boolean isReplica) throws IOException, ClassNotFoundException {
        File folder = new File(serverFolder);
        File[] files = folder.listFiles();

        if (files == null) {
            throw new FileNotFoundException("未找到文件夹或文件夹为空: " + serverFolder);
        }

        for (File file : files) {
            if (file.getName().endsWith(".btree") && file.getName().contains(isReplica ? "replica" : "")) {
                String baseName = file.getName().replace(".btree", "");
                String dataFilePath = new File(folder, baseName + ".lson").getAbsolutePath();

                if (!new File(dataFilePath).exists()) {
                    System.err.println("未找到对应数据文件: " + dataFilePath);
                    continue;
                }

                System.out.println("加载" + (isReplica ? "副本" : "主块") + "索引文件: " + file.getAbsolutePath());
                PersistentBTree<String, Long> bTree = PersistentBTree.loadFromFile(file.getAbsolutePath());
                indexMap.put(file.getAbsolutePath(), bTree);
                dataFileMap.put(file.getAbsolutePath(), dataFilePath);
            }
        }
    }

    /**
     * 使用文件指针从数据文件中读取作者信息。
     */
    private static String fetchAuthorData(Long pointer, String dataFilePath) {
        try (RandomAccessFile raf = new RandomAccessFile(dataFilePath, "r")) {
            raf.seek(pointer);
            return raf.readLine();
        } catch (IOException e) {
            System.err.println("读取数据文件时发生错误: " + e.getMessage());
            return null;
        }
    }
}