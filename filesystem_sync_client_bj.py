import socket
import os
import time
import threading
import hashlib

BJ_DIR = '/ktt/scratch/transfer/sync_from_bj_to_ny5'  # 服务器的文件夹路径
NY5_DIR = '/ktt/scratch/transfer/sync_from_ny5_to_bj'
HOST = '50.248.134.234'  # JumpServer服务器地址
PORT = 65432  # 端口号
USER = 'server'
TIME_INTERVAL = 10

lock = threading.Lock()
os.makedirs(BJ_DIR, exist_ok=True)
os.makedirs(NY5_DIR, exist_ok=True)

def md5(file_path):
    """计算文件的MD5值"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
previous_state = {}
for file_name in os.listdir(BJ_DIR):
    file_path = os.path.join(BJ_DIR, file_name)
    if os.path.isfile(file_path):
        previous_state[file_name] = md5(file_path)

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


def monitor_folder(s):
    """监控sync_from_bj_to_ny5文件夹变化"""
    global previous_state

    # if not previous_state and os.listdir(CLIENT_DIR):
    #     for file_name in os.listdir(CLIENT_DIR):
    #         file_path = os.path.join(CLIENT_DIR, file_name)
    #         if os.path.isfile(file_path):
    #             previous_state[file_name] = md5(file_path)
    # print(previous_state)

    while True:
        current_state = {}
        # if not os.listdir(CLIENT_DIR):
        #     continue
        for file_name in os.listdir(BJ_DIR):
            file_path = os.path.join(BJ_DIR, file_name)
            if os.path.isfile(file_path):
                current_state[file_name] = md5(file_path)

        compare_result = compare_dicts(previous_state, current_state)
        
        if compare_result["added"] or compare_result["removed"] or compare_result["modified"]:
            print(compare_result)
            # 发送更新信息
            # lock.acquire()
            try:
                send_updates(compare_result, s)
            except:
                print("发送更新时客户端连接服务器失败，可能是服务器掉线或端口被占用, 立即返回主线程尝试重连，若不成功则每10秒重连一次")
                # lock.release()
                return
            # lock.release()
            previous_state = current_state
        time.sleep(TIME_INTERVAL)  # 每隔一段时间检查一次文件夹


def send_updates(result:dict, s):
    """将文件夹更新信息发送到服务器"""
    if result["added"]:
        for filename in list(result["added"].keys()):
            file_path = os.path.join(BJ_DIR, filename)
            with open(file_path, 'rb') as f:
                file_data = f.read()
            message = f"ADD|{os.path.basename(file_path)}|{len(file_data)}EOF".encode()
            s.sendall(message)
            exit_code = os.system(f"scp {file_path} {USER}@{HOST}:{BJ_DIR}") #主动从北京向JumpServer发起scp传输文件
            if exit_code == 0:
                s.sendall("ADD Transmission EndEOF".encode())
                print(f"北京客户端操作：从{BJ_DIR}添加文件{filename}，已被同步传输")
            else:
                print(f"北京客户端从{BJ_DIR}传送新添加的文件{filename}失败，等待重连...")
                raise
    elif result["modified"]:
        for filename in list(result["modified"].keys()):
            file_path = os.path.join(BJ_DIR, filename)
            with open(file_path, 'rb') as f:
                file_data = f.read()
            message = f"MODIFY|{os.path.basename(file_path)}|{len(file_data)}EOF".encode()
            s.sendall(message)
            exit_code = os.system(f"scp {file_path} {USER}@{HOST}:{BJ_DIR}") #主动从北京向JumpServer发起scp传输文件
            if exit_code == 0:
                s.sendall("MODIFY Transmission EndEOF".encode())
                print(f"北京客户端操作：从{BJ_DIR}修改文件{filename}，已被同步传输")
            else:
                print(f"北京客户端从{BJ_DIR}传送新修改的文件{filename}失败，等待重连")
                raise
    elif result["removed"]:
        for filename in list(result["removed"].keys()):
            message = f"REMOVE|{filename}|0EOF".encode()
            s.sendall(message)
            print(f"北京客户端操作：在{BJ_DIR}删除文件{filename}，已被同步传输")


def receive_updates(s, addr):
    """接收服务器发来的文件夹更新信息"""
    while True:
        try:
            data = s.recv(1024)
        except:
            print("接收更新时客户端连接服务器失败，可能是服务器掉线或端口被占用, 下次发送更新信息失败后尝试重连，若不成功则每10秒重连一次")
            return
        if not data:
            continue
        list_data = [d for d in data.decode().split('EOF') if d]
        # lock.acquire()
        """根据接收到的更新信息更新本地文件夹"""
        for data in list_data:  
            if data == 'check_conn':
                continue    
            command, filename, file_size = data.split('|') #这里file_data只有开头一部分
            file_size = int(file_size)
            file_path = os.path.join(NY5_DIR, filename)

            if command == "ADD":
                exit_code = os.system(f'scp {USER}@{HOST}:{os.path.join(NY5_DIR, filename)} {os.path.join(NY5_DIR, filename)}')
                if exit_code == 0:
                    print(f"北京客户端{NY5_DIR}文件夹下文件{filename}已被同步添加.")
                else:
                    print(f"北京客户端获取{filename}失败，可能连接断开，尝试修改{BJ_DIR}下文件，触发重连...")
                    return

            elif command == "MODIFY":
                exit_code = os.system(f'scp {USER}@{HOST}:{os.path.join(NY5_DIR, filename)} {os.path.join(NY5_DIR, filename)}')
                if exit_code == 0:
                    print(f"北京客户端{NY5_DIR}文件夹下文件{filename}已被同步添加.")
                else:
                    print(f"北京客户端获取{filename}失败，等待重连...")
                    return 

            elif command == "REMOVE":
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"北京客户端{NY5_DIR}下文件{filename}已被同步删除")

        # lock.release()

init_send = False
def begin_with_sendall(local_dir=BJ_DIR):
    global init_send
    exit_code = os.system(f"rsync -av --delete {local_dir} {USER}@{HOST}:{os.path.dirname(local_dir)}/")
    if exit_code == 0:
        client_socket.sendall("Client Rsync Send Over!".encode())
        init_send = True
        print(f"北京客户端向服务器{BJ_DIR}初始化文件传输结束")
    else:
        client_socket.sendall("Client Rsync ERROR!".encode())   
        print(f"北京客户端向服务器{BJ_DIR}初始化文件传输失败，尝试重连...") 

init_recv = False
def begin_with_recvall(local_dir=NY5_DIR):
    global init_recv
    exit_code = os.system(f"rsync -av --delete {USER}@{HOST}:{local_dir} {os.path.dirname(local_dir)}/")
    if exit_code == 0:
        client_socket.sendall("Client Rsync Recv Over!".encode())
        init_recv = True
        print(f"北京客户端接收服务器{NY5_DIR}初始化文件传输结束")
    else:
        client_socket.sendall("Client Rsync ERROR!".encode())
        print(f"北京客户端接收服务器{NY5_DIR}初始化文件传输失败，尝试重连...")

if __name__ == "__main__":
    # 启动两个线程：监控线程和接收线程
    while True:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((HOST, PORT))
            print(f"北京客户端成功连接服务器{HOST}")
            client_socket.sendall('Bei Jing'.encode())
            time.sleep(1)

            t0 = threading.Thread(target=begin_with_sendall, args=[BJ_DIR])
            t0_ = threading.Thread(target=begin_with_recvall, args=[NY5_DIR])
            t1 = threading.Thread(target=receive_updates, args=[client_socket, HOST])
            t2 = threading.Thread(target=monitor_folder, args=[client_socket])
            
            t0.start()
            t0_.start()
            t0.join()
            t0_.join()

            if not (init_send and init_recv): #初始化失败
                continue

            print(f"北京客户端开始接收来自{HOST}:{NY5_DIR}文件更新信息")
            t1.start() #先接收JumpServer上NY5文件夹中更新的文件信息
            time.sleep(5)
            print(f"北京客户端开始监控{BJ_DIR}文件更新信息，并向{HOST}发送")
            t2.start()

            # t1.join()
            t2.join() #传送更新信息失败时触发重连操作
        
        except:  
            print(f"北京客户端连接服务器{HOST}失败, 10秒后重连, 等待中...")       
            time.sleep(10)
            continue

