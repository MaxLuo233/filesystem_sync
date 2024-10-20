import socket
import os
import time
import threading
import hashlib
import argparse
import logging

logging.basicConfig(
    filename='jumpserver.log',        # 指定日志文件
    level=logging.INFO,        # 设置日志级别
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BJ_DIR = '/home/mluo/sync_from_bj_to_ny5'  # 服务器的文件夹路径
NY5_DIR = '/home/mluo/sync_from_ny5_to_bj'
TIME_INTERVAL = 10
HOST = '0.0.0.0'  # 端口号
PORT = 65432  # 端口号

lock = threading.Lock()
bj_previous_state = {}
ny5_previous_state = {}


def md5(file_path):
    """计算文件的MD5值"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def compare_dicts(old_dict, new_dict):
    # 获取旧字典和新字典中的键
    old_keys = set(old_dict.keys())
    new_keys = set(new_dict.keys())
    # 找到新增的键、删除的键和键值改变的键
    added_keys = new_keys - old_keys
    removed_keys = old_keys - new_keys
    modified_keys = {key for key in old_keys & new_keys if old_dict[key] != new_dict[key]}
    # 输出结果
    result = {
        "added": {key: new_dict[key] for key in added_keys},
        "removed": {key: old_dict[key] for key in removed_keys},
        "modified": {key: {"old": old_dict[key], "new": new_dict[key]} for key in modified_keys}
    }
    return result

def monitor_folder(conn, addr):
    """监控指定文件夹变化"""
    global bj_previous_state
    global ny5_previous_state

    if addr == '北京':
        ny5_previous_state = {}
        monitor_dir = NY5_DIR 
        while True: #开始监控从纽约要往北京发送的文件
            ny5_current_state = {}
            # if not os.listdir(SERVER_DIR):
            #     continue
            for file_name in os.listdir(monitor_dir):
                file_path = os.path.join(monitor_dir, file_name)
                if os.path.isfile(file_path):
                    ny5_current_state[file_name] = md5(file_path)
            
            compare_result = compare_dicts(ny5_previous_state, ny5_current_state) #sync_from_bj_to_ny5状态检测
            
            if compare_result["added"] or compare_result["removed"] or compare_result["modified"]:
                print(f"{monitor_dir}下状态{compare_result}")
                # 发送更新信息
                lock.acquire()            
                try:
                    send_updates(compare_result, conn, monitor_dir, addr)
                except:
                    print(f"来自{addr}的客户端掉线，若长时间未恢复则可能死机，请检查")
                    logging.error(f"来自{addr}的客户端掉线，若长时间未恢复则可能死机，请检查")
                    lock.release()
                    return
                lock.release()
                ny5_previous_state = ny5_current_state
            time.sleep(TIME_INTERVAL)  # 每隔一段时间检查一次文件夹

    elif addr == '纽约':
        bj_previous_state = {}
        monitor_dir = BJ_DIR
        while True: #开始监控从北京要往纽约发送的文件
            bj_current_state = {}
            # if not os.listdir(SERVER_DIR):
            #     continue
            for file_name in os.listdir(monitor_dir):
                file_path = os.path.join(monitor_dir, file_name)
                if os.path.isfile(file_path):
                    bj_current_state[file_name] = md5(file_path)
            
            compare_result = compare_dicts(bj_previous_state, bj_current_state) #sync_from_ny5_to_bj状态检测
            
            if compare_result["added"] or compare_result["removed"] or compare_result["modified"]:
                print(f"{monitor_dir}下状态{compare_result}")
                # 发送更新信息            
                lock.acquire()
                try:
                    send_updates(compare_result, conn, monitor_dir, addr)
                except:
                    print(f"来自{addr}的客户端掉线，若长时间未恢复则可能死机，请检查")
                    logging.error(f"来自{addr}的客户端掉线，若长时间未恢复则可能死机，请检查")
                    lock.release()
                    return
                lock.release()
                bj_previous_state = bj_current_state
            time.sleep(TIME_INTERVAL)  # 每隔一段时间检查一次文件夹
    else:
        print("客户端IP不符合要求, 必须为北京或纽约的主机, 否则不接受客户端的TCP连接请求")
        logging.error("客户端IP不符合要求, 必须为北京或纽约的主机, 否则不接受客户端的TCP连接请求")
        return

def send_updates(result:dict, conn, monitor_dir, addr):
    """将指定的监控的文件夹更新信息发送到客户端"""
    if result["added"]:
        for filename in list(result["added"].keys()):
            file_path = os.path.join(monitor_dir, filename)
            with open(file_path, 'rb') as f:
                file_data = f.read()
            message = f"ADD|{os.path.basename(file_path)}|{len(file_data)}EOF".encode()
            conn.sendall(message)
            print(f"{monitor_dir}添加{filename}的文件信息已发送到{addr}，等待客户端同步...")
            logging.info(f"{monitor_dir}添加{filename}的文件信息已发送到{addr}，等待客户端同步...")
    elif result["modified"]:
        for filename in list(result["modified"].keys()):
            file_path = os.path.join(monitor_dir, filename)
            with open(file_path, 'rb') as f:
                file_data = f.read()
            message = f"MODIFY|{os.path.basename(file_path)}|{len(file_data)}EOF".encode()
            conn.sendall(message)
            print(f"{monitor_dir}修改{filename}的文件信息已发送到{addr}，等待客户端同步...")
            logging.info(f"{monitor_dir}修改{filename}的文件信息已发送到{addr}，等待客户端同步...")
    elif result["removed"]:
        for filename in list(result["removed"].keys()):
            message = f"REMOVE|{filename}|0EOF".encode()
            conn.sendall(message)
            print(f"{monitor_dir}删除{filename}的文件信息已发送到{addr}，等待客户端同步...")
            logging.info(f"{monitor_dir}删除{filename}的文件信息已发送到{addr}，等待客户端同步...")

def receive_updates(conn, addr):
    """接收客户端发来的文件夹更新信息, 并根据BJ还是NYU采取不同的逻辑"""
    while True:
        try:
            data = conn.recv(1024)
        except:
            print(f"来自{addr}的客户端掉线，若长时间未恢复则可能死机，请检查")
            logging.error(f"来自{addr}的客户端掉线，若长时间未恢复则可能死机，请检查")
            return
        if not data:
            continue
        list_data = [d for d in data.decode().split('EOF') if d]
        if not list_data:
            continue
        lock.acquire()
        """根据接收到的更新信息更新本地文件夹"""
        for data in list_data: 
            if data == "ADD Transmission End":
                print(f"来自{addr}的添加文件{filename}已被同步添加.") 
                logging.info(f"来自{addr}的添加文件{filename}已被同步添加.")
            elif data == "MODIFY Transmission End":
                print(f"来自{addr}的修改文件{filename}已被同步修改.") 
                logging.info(f"来自{addr}的修改文件{filename}已被同步修改.")
            else:        
                command, filename, file_size = data.split('|') #这里file_data只有开头一部分
                file_size = int(file_size)
                if addr == '北京':
                    file_path = os.path.join(BJ_DIR, filename)
                elif addr == '纽约':
                    file_path = os.path.join(NY5_DIR, filename)

                if command == "REMOVE":
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        print(f"{os.path.basename(file_path)}下文件{filename}已被同步删除")
                        logging.info(f"{os.path.basename(file_path)}下文件{filename}已被同步删除")

        lock.release()


def check_conn(conn, addr):
    time.sleep(5)
    while True:
        try:
            conn.sendall(b'check_connEOF')
            time.sleep(5)
        except:
            print(f"来自{addr}的客户端掉线，若长时间未恢复则可能死机，请检查")
            logging.error(f"来自{addr}的客户端掉线，若长时间未恢复则可能死机，请检查")
            return

init_rsync = False
def begin_rsync(conn):
    global init_rsync
    while True:
        try:
            data = conn.recv(1024).decode()
            if data == "Client Rsync Recv Over!":
                init_rsync = True
                return 
            if data == "Client Rsync ERROR!":
                print("客户端初始化文件同步失败，等待重连...")
                logging.error("客户端初始化文件同步失败，等待重连...")
                return
        except:
            return 

if __name__ == "__main__":
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(1)
    print(f"{HOST}服务器在{PORT}端口等待连接...")
    logging.info(f"{HOST}服务器在{PORT}端口等待连接...")

    while True:
        conn, ip_addr = server_socket.accept()
        print(f"来自{ip_addr}客户端的连接建立")
        logging.info(f"来自{ip_addr}客户端的连接建立")

        data = conn.recv(1024).decode()
        addr = ''
        if data == 'Bei Jing':
            addr = '北京'
            print("客户端同步请求来自北京")
            logging.info("客户端同步请求来自北京")
        elif data == 'New York':
            addr = '纽约'
            print("客户端同步请求来自纽约")
            logging.info("客户端同步请求来自纽约")

        # 启动两个线程：监控线程和接收线程
        
        t0 = threading.Thread(target=begin_rsync, args=[conn])
        t1 = threading.Thread(target=monitor_folder, args=[conn, addr])
        t2 = threading.Thread(target=receive_updates, args=[conn, addr]) #注意先后顺序
        t3 = threading.Thread(target=check_conn, args=[conn, addr])

        lock.acquire()
        t0.start()
        t0.join()  # 必须先跟客户端的本地文件夹同步再进行后面的工作
        if not init_rsync:
            lock.release()
            print(f"来自{addr}的客户端初始化文件同步失败，可能是客户端掉线，若长时间未恢复则可能死机，请检查")
            logging.error(f"来自{addr}的客户端初始化文件同步失败，可能是客户端掉线，若长时间未恢复则可能死机，请检查")
            continue
        print(f"与{addr}客户端文件初始化同步结束")
        logging.info(f"与{addr}客户端文件初始化同步结束")
        lock.release()

        if addr == 'Bei Jing':
            ny5_previous_state = {}
            print(f"开始监控{NY5_DIR},向{addr}客户端发送文件夹更新信息") 
            logging.info(f"开始监控{NY5_DIR},向{addr}客户端发送文件夹更新信息")
            t1.start()
            time.sleep(5)   
            print(f"开始接收来自{addr}客户端的更新信息，同步{BJ_DIR}文件夹")   
            logging.info(f"开始接收来自{addr}客户端的更新信息，同步{BJ_DIR}文件夹")     
            t2.start()
        elif addr == 'New York':
            bj_previous_state = {}
            print(f"开始监控{BJ_DIR},向{addr}客户端发送文件夹更新信息")
            logging.info(f"开始监控{BJ_DIR},向{addr}客户端发送文件夹更新信息") 
            t1.start()
            time.sleep(5)   
            print(f"开始接收来自{addr}客户端的更新信息，同步{NY5_DIR}文件夹")   
            logging.info(f"开始接收来自{addr}客户端的更新信息，同步{NY5_DIR}文件夹")     
            t2.start()

        
        t3.start() #如果写try: t3.join()，则t3运行异常后会直接导致主程序失败，不会进入except