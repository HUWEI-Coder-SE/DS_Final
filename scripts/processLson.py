import json
import re
import os
import sys

def process_individual_json(line):
    """
    解析单行 JSON 数据。

    参数：
        line (str): JSON 格式的字符串。

    返回：
        dict: 解析后的 JSON 数据。
    """
    return json.loads(line)

def shuffle_and_save_json(file_path):
    """
    读取 JSON 文件内容，随机打乱顺序后保存为新文件。

    参数：
        file_path (str): 输入的 JSON 文件路径。
    """
    # 读取文件内容并解析为 JSON 对象列表
    with open(file_path, 'r', encoding='utf-8') as file:
        json_data_list = [process_individual_json(line.strip()) for line in file]

    # 随机打乱顺序
    import random
    random.shuffle(json_data_list)

    # 生成新的文件名并保存结果
    base_path, file_name = os.path.split(file_path)
    file_name_without_extension, file_extension = os.path.splitext(file_name)
    new_file_name = f"{file_name_without_extension}_processed{file_extension}"
    new_file_path = os.path.join(base_path, new_file_name)

    with open(new_file_path, 'w', encoding='utf-8') as file:
        for data in json_data_list:
            file.write(json.dumps(data) + '\n')

    print(f"随机打乱后的数据已保存至: {new_file_path}")

if __name__ == "__main__":
    # 检查命令行参数数量是否正确
    if len(sys.argv) != 2:
        print("用法: python processLson.py <json_file_path>")
        sys.exit(1)

    json_file_path = sys.argv[1]
    shuffle_and_save_json(json_file_path)
