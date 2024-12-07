import sys
import os
import json

def custom_hash_function(name):
    """
    自定义哈希函数，根据姓名的 ASCII 值计算哈希值。

    参数：
        name (str): 姓名字符串。

    返回：
        int: 计算得到的哈希值。
    """
    hash_value = sum(ord(char) for char in name)
    return hash_value

class HashBuffer:
    """
    哈希缓冲区，用于批量写入文件。
    """
    def __init__(self, filename):
        self.filename = filename
        self.buffer = []
        self.buffer_size = 500  # 缓冲区大小，可根据实际需求调整

    def add_to_buffer(self, data):
        """
        将数据添加到缓冲区，如果缓冲区已满则写入文件。

        参数：
            data (dict): 需要写入的数据。
        """
        self.buffer.append(data)
        if len(self.buffer) >= self.buffer_size:
            self.flush_buffer()

    def flush_buffer(self):
        """
        将缓冲区中的数据写入文件，并清空缓冲区。
        """
        with open(self.filename, 'a', encoding='utf-8') as file:
            for item in self.buffer:
                file.write(json.dumps(item) + '\n')
            self.buffer = []

def distribute_to_buckets(file_path, bucket_count):
    """
    将输入文件按照哈希值分配到多个桶文件中。

    参数：
        file_path (str): 输入文件路径。
        bucket_count (int): 桶的数量。
    """
    file_name_without_extension, _ = os.path.splitext(file_path)

    # 创建缓冲区对象
    buffers = [HashBuffer(f'{file_name_without_extension}_bucket_{i}.lson') for i in range(bucket_count)]

    # 读取原始数据文件并分配到桶
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data = json.loads(line)
            name = list(data.keys())[0]  # 获取姓名作为哈希键
            hash_value = custom_hash_function(name)
            bucket_index = hash_value % bucket_count
            buffers[bucket_index].add_to_buffer(data)

    # 写入所有缓冲区中的剩余数据
    for buffer in buffers:
        buffer.flush_buffer()

    print(f"成功将文件分配到 {bucket_count} 个桶中！")

if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) != 3:
        print("用法: python setHashIndex.py <file_path> <bucket_count>")
        sys.exit(1)

    file_path = sys.argv[1]
    bucket_count = int(sys.argv[2])

    # 分配数据到桶
    distribute_to_buckets(file_path, bucket_count)
