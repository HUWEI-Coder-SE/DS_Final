package client;

import utils.ReplicaManager;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * RequestThread 类，负责向存储虚拟机发送查询请求并返回结果。
 */
public class RequestThread implements Callable<Map<Integer, Integer>> {

    private final ReplicaManager replicaManager;
    private final String fileName;
    private final String author;

    public RequestThread(ReplicaManager replicaManager, String fileName, String author) {
        this.replicaManager = replicaManager;
        this.fileName = fileName;
        this.author = author;
    }

    @Override
    public Map<Integer, Integer> call() {
        Map<Integer, Integer> result = new HashMap<>();
        boolean isQueried = false;

        // 优先查询主块
        int primaryServer = replicaManager.getPrimaryServer(fileName);
        if (primaryServer != -1) {
            isQueried = queryServer(primaryServer, result);
        }

        // 如果主块不可用，查询副本
        if (!isQueried) {
            int replicaServer = replicaManager.getAvailableReplica(fileName);
            if (replicaServer != -1) {
                isQueried = queryServer(replicaServer, result);
            }
        }

        // 如果查询成功，标记文件为已查询
        if (isQueried) {
            replicaManager.markFileAsQueried(fileName);
        } else {
            System.out.println("文件 " + fileName + " 的主块和副本都不可用，跳过查询。");
        }

        return result;
    }

    /**
     * 向指定存储机查询文件数据。
     *
     * @param serverId 存储机编号。
     * @param result   查询结果映射。
     * @return 是否查询成功。
     */
    private boolean queryServer(int serverId, Map<Integer, Integer> result) {
        String serverAddress = replicaManager.getServerAddress(serverId);
        if (serverAddress == null || serverAddress.isEmpty()) {
            return false;
        }

        String[] parts = serverAddress.split(":");
        String serverIP = parts[0];
        int port = Integer.parseInt(parts[1]);

        try (Socket socket = new Socket(serverIP, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // 发送查询请求
            out.println(author);

            // 接收服务器响应
            StringBuilder responseBuilder = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                responseBuilder.append(line);
            }

            String response = responseBuilder.toString();

            if (response.startsWith("未找到作者")) {
                System.out.println("服务器 " + serverIP + ":" + port + " 未找到作者: " + author);
                return false;
            }

            // 解析响应并填充结果
            result.putAll(parseResponse(response));
            return true;

        } catch (IOException e) {
            System.err.println("与存储机通信时发生错误: " + e.getMessage());
            return false;
        } catch (Exception e) {
            System.err.println("解析服务器响应时发生错误: " + e.getMessage());
            return false;
        }
    }

    /**
     * 解析服务器返回的嵌套 JSON 格式字符串。
     *
     * @param response 服务器返回的 JSON 格式字符串。
     * @return 解析后的年份和数量的映射。
     * @throws Exception 如果数据格式错误或无法解析。
     */
    private Map<Integer, Integer> parseResponse(String response) throws Exception {
        Map<Integer, Integer> yearData = new HashMap<>();

        // 验证响应格式
        if (!response.startsWith("{") || !response.endsWith("}")) {
            throw new Exception("无效的服务器返回格式: " + response);
        }

        // 去掉外层的花括号
        String content = response.substring(1, response.length() - 1).trim();

        // 找到作者部分的起点
        int authorStart = content.indexOf("\"" + author + "\"");
        if (authorStart == -1) {
            throw new Exception("未找到作者 " + author + " 的数据");
        }

        // 提取作者后面的内容
        String authorData = content.substring(authorStart + author.length() + 4); // 跳过: "author":
        if (!authorData.startsWith("{") || !authorData.endsWith("}")) {
            throw new Exception("作者数据格式错误: " + authorData);
        }

        // 去掉作者数据的花括号
        String yearContent = authorData.substring(1, authorData.length() - 1).trim();

        // 按逗号分割年份数据
        String[] yearEntries = yearContent.split(",");
        for (String entry : yearEntries) {
            try {
                // 验证每个键值对格式
                String[] parts = entry.split(":");
                if (parts.length != 2) {
                    System.err.println("跳过无效的年份数据: " + entry);
                    continue; // 跳过无效数据
                }

                // 提取键值
                String yearString = parts[0].trim().replace("\"", "");
                String countString = parts[1].trim();

                // 验证年份是否为数字
                if (!yearString.matches("\\d+")) {
                    System.err.println("跳过无效年份: " + yearString);
                    continue;
                }

                // 转换为整数并存储
                int year = Integer.parseInt(yearString);
                int count = Integer.parseInt(countString);
                yearData.put(year, count);
            } catch (NumberFormatException e) {
                System.err.println("跳过格式错误的年份数据: " + entry);
            }
        }

        return yearData;
    }
}