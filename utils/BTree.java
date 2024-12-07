package utils;

import java.io.Serializable;

import java.io.*;
import java.util.*;

/**
 * BTree 类实现了一个简单的 B 树结构，用于存储和检索键值对。
 * 提供高效的插入、搜索功能，并支持持久化存储。
 */
public class BTree implements Serializable {

    private static final long serialVersionUID = 1L;

    // 根节点
    private Node root;

    // 每个节点的最小度数（每个节点至少包含 degree - 1 个键）
    private final int degree;

    /**
     * 节点类，表示 BTree 的每个节点。
     */
    private static class Node implements Serializable {
        private static final long serialVersionUID = 1L;

        boolean isLeaf; // 是否是叶子节点
        List<String> keys; // 键的列表
        List<List<Long>> values; // 键对应的值（文件指针列表）
        List<Node> children; // 子节点列表

        Node(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.keys = new ArrayList<>();
            this.values = new ArrayList<>();
            this.children = new ArrayList<>();
        }
    }

    /**
     * 构造函数，初始化一个空的 BTree。
     * @param degree 树的度数，必须大于等于 2。
     */
    public BTree(int degree) {
        if (degree < 2) {
            throw new IllegalArgumentException("度数必须大于等于 2");
        }
        this.degree = degree;
        this.root = new Node(true);
    }

    /**
     * 插入一个键值对到 BTree 中。
     * @param key 键。
     * @param value 值，通常是文件指针。
     */
    public void insert(String key, Long value) {
        Node r = root;
        if (r.keys.size() == 2 * degree - 1) { // 如果根节点满了
            Node s = new Node(false);
            root = s;
            s.children.add(r);
            splitChild(s, 0, r);
            insertNonFull(s, key, value);
        } else {
            insertNonFull(r, key, value);
        }
    }

    /**
     * 在非满节点中插入键值对。
     * @param node 当前节点。
     * @param key 键。
     * @param value 值。
     */
    private void insertNonFull(Node node, String key, Long value) {
        int i = node.keys.size() - 1;
        if (node.isLeaf) { // 如果是叶子节点
            while (i >= 0 && key.compareTo(node.keys.get(i)) < 0) {
                i--;
            }
            node.keys.add(i + 1, key);
            node.values.add(i + 1, new ArrayList<>(Collections.singletonList(value)));
        } else { // 如果是内部节点
            while (i >= 0 && key.compareTo(node.keys.get(i)) < 0) {
                i--;
            }
            i++;
            Node child = node.children.get(i);
            if (child.keys.size() == 2 * degree - 1) { // 如果子节点满了
                splitChild(node, i, child);
                if (key.compareTo(node.keys.get(i)) > 0) {
                    i++;
                }
            }
            insertNonFull(node.children.get(i), key, value);
        }
    }

    /**
     * 分裂一个满节点。
     * @param parent 父节点。
     * @param index 父节点中子节点的索引。
     * @param fullChild 要分裂的子节点。
     */
    private void splitChild(Node parent, int index, Node fullChild) {
        Node newChild = new Node(fullChild.isLeaf);

        // 将中间键上移到父节点
        parent.keys.add(index, fullChild.keys.remove(degree - 1));
        parent.values.add(index, fullChild.values.remove(degree - 1));
        parent.children.add(index + 1, newChild);

        // 将右半部分的键和值移动到新节点
        for (int j = 0; j < degree - 1; j++) {
            newChild.keys.add(fullChild.keys.remove(degree - 1));
            newChild.values.add(fullChild.values.remove(degree - 1));
        }

        // 如果不是叶子节点，还需要移动子节点
        if (!fullChild.isLeaf) {
            for (int j = 0; j < degree; j++) {
                newChild.children.add(fullChild.children.remove(degree));
            }
        }
    }

    /**
     * 搜索一个键对应的值。
     * @param key 要搜索的键。
     * @return 键对应的文件指针列表，如果键不存在则返回 null。
     */
    public List<Long> search(String key) {
        return search(root, key);
    }

    private List<Long> search(Node node, String key) {
        int i = 0;
        while (i < node.keys.size() && key.compareTo(node.keys.get(i)) > 0) {
            i++;
        }
        if (i < node.keys.size() && key.equals(node.keys.get(i))) {
            return node.values.get(i);
        } else if (node.isLeaf) {
            return null;
        } else {
            return search(node.children.get(i), key);
        }
    }

    /**
     * 打印树结构（调试用）。
     */
    public void print() {
        print(root, 0);
    }

    private void print(Node node, int level) {
        System.out.println("层级 " + level + ": " + node.keys);
        if (!node.isLeaf) {
            for (Node child : node.children) {
                print(child, level + 1);
            }
        }
    }

    /**
     * 将 BTree 持久化到文件中。
     * @param filePath 文件路径。
     */
    public void saveToFile(String filePath) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeObject(this);
        }
    }

    /**
     * 从文件中加载 BTree。
     * @param filePath 文件路径。
     * @return 加载的 BTree 实例。
     */
    public static BTree loadFromFile(String filePath) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            return (BTree) ois.readObject();
        }
    }
}
