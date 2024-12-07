import re
import json
import os

def extract_author_and_year(file_path, output_dir):
    """
    从给定的 XML 文件中提取作者和年份信息，并保存为 LSON 格式。

    参数：
        file_path (str): 输入 XML 文件的路径。
        output_dir (str): 保存输出 LSON 文件的目录。
    """
    data = {}

    # 打开 XML 文件并逐行读取
    with open(file_path, 'r', encoding='utf-8') as file:
        authors = []  # 存储当前论文的作者列表
        for line in file:
            # 检测新的出版物块的开始
            if "key=" in line:
                authors = []

            # 提取作者名称
            if "<author" in line:
                match = re.search(r'<author[\s\S]*?>([\s\S]*?)</author>', line, re.M | re.I)
                if match:
                    authors.append(match.group(1).strip())

            # 提取年份并更新数据结构
            if "<year>" in line:
                match = re.search(r'<year>(\d+)</year>', line, re.M | re.I)
                if match:
                    year = int(match.group(1))
                    for author in authors:
                        if author not in data:
                            data[author] = {}
                        if year not in data[author]:
                            data[author][year] = 0
                        data[author][year] += 1

    # 准备输出文件路径
    file_name_without_extension = os.path.splitext(os.path.basename(file_path))[0]
    output_file_path = os.path.join(output_dir, f"{file_name_without_extension}_line.lson")

    # 将数据写入 LSON 文件
    with open(output_file_path, 'w', encoding='utf-8') as output_file:
        for author, years in data.items():
            json_line = json.dumps({author: years}, separators=(',', ':'))
            output_file.write(json_line + '\n')

    print(f"处理完成，输出保存至: {output_file_path}")

# 示例用法
if __name__ == "__main__":
    input_file = "data/dblp.xml"  # 替换为实际的输入文件路径
    output_directory = "data"  # 替换为实际的输出目录

    # 如果输出目录不存在，则创建
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    extract_author_and_year(input_file, output_directory)
