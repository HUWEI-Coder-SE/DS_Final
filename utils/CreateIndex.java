package utils;

import java.io.*;
import java.util.*;
import utils.BTree;
import utils.PersistentBTree;

/**
 * CreateIndex 类用于创建和保存 PersistentBTree 索引。
 */
public class CreateIndex {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("用法: java CreateIndex <输入文件路径> <索引文件路径>");
            return;
        }

        String inputFilePath = args[0]; // 输入的桶文件路径
        String indexFilePath = args[1]; // 输出的索引文件路径

        try {
            // 创建 PersistentBTree 索引
            PersistentBTree<String, Long> bTree = createPersistentBTreeIndex(inputFilePath);

            // 将索引保存到文件
            bTree.saveToFile(indexFilePath);
            System.out.println("索引文件已成功创建: " + indexFilePath);
        } catch (IOException e) {
            System.err.println("发生错误: " + e.getMessage());
        }
    }

    /**
     * 创建 PersistentBTree 索引。
     *
     * @param inputFilePath 输入文件路径。
     * @return 构建好的 PersistentBTree 实例。
     * @throws IOException 如果读取文件时发生错误。
     */
    public static PersistentBTree<String, Long> createPersistentBTreeIndex(String inputFilePath) throws IOException {
        // 创建 PersistentBTree 实例，假设度数为 4
        PersistentBTree<String, Long> bTree = new PersistentBTree<>(4);

        // 打开输入文件并逐行读取
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            long filePointer = 0; // 文件指针位置

            while ((line = reader.readLine()) != null) {
                // 假设每行是 JSON 格式数据：{"name": {"year": count}}
                int separatorIndex = line.indexOf(':'); // 找到第一个冒号的位置
                if (separatorIndex == -1) continue; // 跳过无效行

                // 提取键（姓名）
                String key = line.substring(2, separatorIndex - 1).trim();

                // 将键和值（文件指针位置）插入到 PersistentBTree
                bTree.insert(key, filePointer);

                // 更新文件指针，假设每行以 \n 结束
                filePointer += line.getBytes().length + 1;
            }
        }

        return bTree;
    }
}