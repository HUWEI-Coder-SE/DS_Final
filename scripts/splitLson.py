import os

def split_file(file_name, chunk_count):
    """
    将一个大文件按照指定的块数进行分割，并存储到对应的服务器文件夹中，每个文件夹包含其他两块的副本。

    参数：
        file_name (str): 输入文件的路径。
        chunk_count (int): 分割的块数。
    """
    try:
        # 读取原始文件的所有行
        with open(file_name, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # 计算每个块的行数
        lines_per_chunk = len(lines) // chunk_count
        remainder_lines = len(lines) % chunk_count

        # 创建服务器文件夹
        server_dirs = [f"server{i+1}" for i in range(chunk_count)]
        for server_dir in server_dirs:
            os.makedirs(server_dir, exist_ok=True)

        # 分割文件并存储
        chunks = []
        start = 0
        for i in range(chunk_count):
            # 确定当前块的行数
            chunk_size = lines_per_chunk + (1 if i < remainder_lines else 0)
            chunk = lines[start:start + chunk_size]

            # 保存块数据到内存
            chunks.append(chunk)
            start += chunk_size

        # 写入块文件和副本到对应的服务器目录
        for i, chunk in enumerate(chunks):
            primary_server = server_dirs[i]
            primary_file_name = os.path.join(primary_server, f"chunk_{i+1}.lson")

            # 写入主块文件
            with open(primary_file_name, 'w', encoding='utf-8') as primary_file:
                primary_file.writelines(chunk)
            print(f"生成主块文件: {primary_file_name}")

            # 写入副本到其他两个服务器
            replica_servers = [server_dirs[(i+j+1) % chunk_count] for j in range(2)]
            for j, replica_server in enumerate(replica_servers):
                replica_file_name = os.path.join(replica_server, f"chunk_{i+1}_replica{j+1}.lson")
                with open(replica_file_name, 'w', encoding='utf-8') as replica_file:
                    replica_file.writelines(chunk)
                print(f"生成副本文件: {replica_file_name}")

        print(f"成功将文件分割为 {chunk_count} 个块文件，并分发到服务器文件夹中！")

    except FileNotFoundError:
        print("错误: 输入文件不存在！")
    except Exception as e:
        print(f"出现错误: {e}")

if __name__ == "__main__":
    import sys

    # 检查命令行参数
    if len(sys.argv) != 3:
        print("用法: python splitLson.py <file_name> <chunk_count>")
        sys.exit(1)

    # 获取命令行参数
    file_name = sys.argv[1]
    chunk_count = int(sys.argv[2])

    # 执行分割操作
    split_file(file_name, chunk_count)
