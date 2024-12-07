package utils;
import java.io.*;
import java.util.*;

/**
 * PersistentBTree 类提供了一个支持持久化的 B 树实现。
 * 通过序列化和反序列化，将 B 树保存到文件并重新加载。
 */
public class PersistentBTree<K extends Comparable<K>, V> implements Serializable {

    private static final long serialVersionUID = 1L;

    // 根节点
    private Node<K, V> root;

    // 每个节点的度数
    private final int degree;

    /**
     * 节点类，用于表示 B 树的每个节点。
     */
    private static class Node<K, V> implements Serializable {
        private static final long serialVersionUID = 1L;

        boolean isLeaf; // 是否是叶子节点
        List<K> keys; // 键列表
        List<V> values; // 值列表
        List<Node<K, V>> children; // 子节点列表

        Node(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.keys = new ArrayList<>();
            this.values = new ArrayList<>();
            this.children = new ArrayList<>();
        }
    }

    /**
     * 构造一个新的 PersistentBTree。
     *
     * @param degree B 树的度数，必须大于等于 2。
     */
    public PersistentBTree(int degree) {
        if (degree < 2) {
            throw new IllegalArgumentException("度数必须大于等于 2");
        }
        this.degree = degree;
        this.root = new Node<>(true);
    }

    /**
     * 插入键值对。
     *
     * @param key 键。
     * @param value 值。
     */
    public void insert(K key, V value) {
        Node<K, V> r = root;
        if (r.keys.size() == 2 * degree - 1) {
            Node<K, V> s = new Node<>(false);
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
     */
    private void insertNonFull(Node<K, V> node, K key, V value) {
        int i = node.keys.size() - 1;
        if (node.isLeaf) {
            while (i >= 0 && key.compareTo(node.keys.get(i)) < 0) {
                i--;
            }
            node.keys.add(i + 1, key);
            node.values.add(i + 1, value);
        } else {
            while (i >= 0 && key.compareTo(node.keys.get(i)) < 0) {
                i--;
            }
            i++;
            Node<K, V> child = node.children.get(i);
            if (child.keys.size() == 2 * degree - 1) {
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
     */
    private void splitChild(Node<K, V> parent, int index, Node<K, V> fullChild) {
        Node<K, V> newChild = new Node<>(fullChild.isLeaf);
        parent.keys.add(index, fullChild.keys.remove(degree - 1));
        parent.values.add(index, fullChild.values.remove(degree - 1));
        parent.children.add(index + 1, newChild);

        for (int j = 0; j < degree - 1; j++) {
            newChild.keys.add(fullChild.keys.remove(degree - 1));
            newChild.values.add(fullChild.values.remove(degree - 1));
        }
        if (!fullChild.isLeaf) {
            for (int j = 0; j < degree; j++) {
                newChild.children.add(fullChild.children.remove(degree));
            }
        }
    }

    /**
     * 搜索键对应的值。
     *
     * @param key 要搜索的键。
     * @return 对应的值，如果不存在则返回 null。
     */
    public V search(K key) {
        return search(root, key);
    }

    private V search(Node<K, V> node, K key) {
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
     * 将 BTree 持久化到文件中。
     *
     * @param filePath 文件路径。
     * @throws IOException 如果写入文件时发生错误。
     */
    public void saveToFile(String filePath) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeObject(this);
        }
    }

    /**
     * 从文件中加载 BTree。
     *
     * @param filePath 文件路径。
     * @return 加载的 BTree 实例。
     * @throws IOException 如果读取文件时发生错误。
     * @throws ClassNotFoundException 如果反序列化失败。
     */
    public static <K extends Comparable<K>, V> PersistentBTree<K, V> loadFromFile(String filePath) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            @SuppressWarnings("unchecked")
            PersistentBTree<K, V> bTree = (PersistentBTree<K, V>) ois.readObject();
            return bTree;
        }
    }

    /**
     * 打印树的内容，调试用。
     */
    public void print() {
        print(root, 0);
    }

    private void print(Node<K, V> node, int level) {
        System.out.println("层级 " + level + ": " + node.keys);
        if (!node.isLeaf) {
            for (Node<K, V> child : node.children) {
                print(child, level + 1);
            }
        }
    }
}