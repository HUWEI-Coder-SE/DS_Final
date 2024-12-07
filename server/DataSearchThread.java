package server;

import utils.PersistentBTree;
import java.io.*;
import java.net.*;
import java.util.HashSet;
import java.util.Set;

/**
 * DataSearchThread 类实现服务器端的查询逻辑，
 * 通过索引快速响应客户端请求，避免副本数据的重复查询。
 */
public class DataSearchThread implements Runnable {

    private final Socket clientSocket; // 客户端连接的 Socket
    private final PersistentBTree<String, Long> bTree; // B 树索引
    private final String dataFilePath; // 数据文件路径
    private static final Set<String> queriedAuthors = new HashSet<>(); // 避免重复查询的记录

    /**
     * 构造函数，初始化线程。
     *
     * @param clientSocket 客户端连接的 Socket。
     * @param bTree B 树索引。
     * @param dataFilePath 数据文件路径。
     */
    public DataSearchThread(Socket clientSocket, PersistentBTree<String, Long> bTree, String dataFilePath) {
        this.clientSocket = clientSocket;
        this.bTree = bTree;
        this.dataFilePath = dataFilePath;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String author = in.readLine(); // 读取客户端查询
            System.out.println("客户端查询: " + author);

            synchronized (queriedAuthors) {
                if (queriedAuthors.contains(author)) {
                    out.println("重复查询，已忽略: " + author);
                    return;
                }
                queriedAuthors.add(author);
            }

            Long filePointer = bTree.search(author); // 查询索引

            if (filePointer == null) {
                out.println("未找到作者: " + author);
            } else {
                String authorData = fetchAuthorData(filePointer); // 读取数据文件
                out.println(authorData);
            }

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
     * 使用文件指针从数据文件中获取作者信息。
     *
     * @param filePointer 文件指针。
     * @return 作者信息。
     */
    private String fetchAuthorData(Long filePointer) {
        try (RandomAccessFile raf = new RandomAccessFile(dataFilePath, "r")) {
            raf.seek(filePointer);
            return raf.readLine();
        } catch (IOException e) {
            System.err.println("读取作者数据时发生错误: " + e.getMessage());
            return "错误: 无法读取作者数据。";
        }
    }
}
