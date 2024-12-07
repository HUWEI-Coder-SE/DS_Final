package server;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

/**
 * MainServer2 类实现基于全局扫描的请求处理。
 * 启动服务器以监听客户端请求，并通过全局扫描响应查询。
 */
public class MainServer2 {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("用法: java MainServer2 <端口号> <数据文件路径>");
            return;
        }

        int port = Integer.parseInt(args[0]); // 服务器监听的端口号
        String dataFilePath = args[1]; // 数据文件路径

        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(5);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("服务器已启动，正在监听端口: " + port);

            while (true) {
                // 接受客户端连接
                Socket clientSocket = serverSocket.accept();
                System.out.println("接受到客户端连接: " + clientSocket.getInetAddress());

                // 将查询任务提交到线程池
                executor.submit(() -> handleClientRequest(clientSocket, dataFilePath));
            }
        } catch (IOException e) {
            System.err.println("服务器运行时发生错误: " + e.getMessage());
        } finally {
            executor.shutdown(); // 关闭线程池
        }
    }

    /**
     * 处理客户端请求，通过全局扫描查询数据。
     *
     * @param clientSocket 客户端连接的 Socket。
     * @param dataFilePath 数据文件路径。
     */
    private static void handleClientRequest(Socket clientSocket, String dataFilePath) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            // 读取客户端发送的查询
            String author = in.readLine();
            System.out.println("客户端查询: " + author);

            // 扫描数据文件获取结果
            String result = scanDataFileForAuthor(dataFilePath, author);

            // 返回结果给客户端
            out.println(result);

        } catch (IOException e) {
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
     * 扫描数据文件查找作者信息。
     *
     * @param dataFilePath 数据文件路径。
     * @param author 查询的作者名。
     * @return 查询结果，如果未找到则返回 "未找到作者"。
     */
    private static String scanDataFileForAuthor(String dataFilePath, String author) {
        try (BufferedReader reader = new BufferedReader(new FileReader(dataFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains(author)) {
                    return line;
                }
            }
        } catch (IOException e) {
            System.err.println("扫描数据文件时发生错误: " + e.getMessage());
        }
        return "未找到作者: " + author;
    }
}
