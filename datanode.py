import sys
import socket
import concurrent.futures
import os
import shutil
import time
import random
from pathlib import Path


class Datanode:
    def __init__(self, dn_id):
        self.dn_id = int(dn_id)                                                     # datanode ID
        self.active_datanodes = None
        dir_name = "datanode" + dn_id
        os.makedirs(dir_name, exist_ok=True)
        self.rep_factor = 3

        self.port = 3000 + self.dn_id                                               # port number for communicating with clients
        self.host = socket.gethostname()                                            # host
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)             # TCP socket for communicating with clients
        self.socket.bind((self.host, self.port)) 

        self.namenode_port = 5000                                                   # namenode port number
        self.namenode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    # TCP socket for communicating with namenode

        self.namenode_socket.connect((self.host, self.namenode_port))               # connect to the namenode
        self.namenode_socket.send(f"datanode{self.dn_id}".encode())                 # send self identifying message


    def send_block_by_id(self, file_id, block_id, client_sock):
        block_file_name = f"datanode{self.dn_id}/file{file_id}/block{block_id}.txt"
        if not os.path.exists(block_file_name):
            print(f"Block {block_id} for File ID {file_id} not found.")
            client_sock.send(b"Block not found")
            return

        with open(block_file_name, 'rb') as block_file:
            block_data = block_file.read()
            client_sock.send(block_data)
            print(f"Sent block {block_id} of File ID {file_id} to client.")

################################################################################################################
    def stream_block_by_id_line_by_line(self, file_id, block_id, client_sock):
        block_file_name = f"datanode{self.dn_id}/file{file_id}/block{block_id}.txt"
        if not os.path.exists(block_file_name):
            print(f"Block {block_id} for File ID {file_id} not found.")
            client_sock.send(b"Block not found")
            return

        with open(block_file_name, 'rb') as block_file:
            for line in block_file:
                client_sock.send(line)
                # Wait for client acknowledgment to send the next line
                ack = client_sock.recv(1024)
                if ack != b'ACK':
                    break
            print(f"Finished streaming block {block_id} of File ID {file_id}.")


    def ping_ack(self):
        ping = self.namenode_socket.recv(1024).decode()                             # receive ping from namenode

        while ping:
            self.namenode_socket.send("ack".encode())                               # send acknowledgement
            ping = self.namenode_socket.recv(1024).decode() 

        self.namenode_socket.close()                                                # close the connection
        
    def update_metadata(self, file_id, block_id):
        temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp.connect((self.host, 5000))
        temp.send("client".encode())
        time.sleep(2)
        temp.send(f"metadata_update {file_id} {block_id} {self.dn_id}".encode())
        res = int(temp.recv(1024).decode())
        temp.send("bye".encode())
        temp.close()

    

    def get_active_datanodes(self):
        temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp.connect((self.host, 5000))
        temp.send("client".encode())
        time.sleep(2)
        temp.send("active datanodes".encode())
        slist = temp.recv(1024).decode()
        temp.send("bye".encode())
        temp.close()
        self.active_datanodes = slist[1:-1].split(", ")
        self.active_datanodes = list(map(int, self.active_datanodes))
        pass
        


    def replicate(self, block, file_id, block_id):

        if len(self.active_datanodes) < self.rep_factor:
            if len(self.active_datanodes == 1):
                print("No other datanodes available, aborting replication")
                return
            rep = len(self.active_datanodes)
            print("Not enough datanodes available, new rep factor: ", rep)
        else:
            rep = self.rep_factor
        
        self.active_datanodes.remove(self.dn_id)
        n = len(self.active_datanodes)

        i = random.randint(0,n-1)
        for k in range(rep-1):
            temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp.connect((self.host, 3000 + self.active_datanodes[i]))
            init_msg = f"save {file_id} {block_id}"
            temp.send(init_msg.encode())
            time.sleep(2)

            temp.send(block.encode())
            status = int(temp.recv(1024).decode())
            if status:
                print(f"\trep {k + 1} of {file_id}_{block_id} successful")
            temp.close()

            i = (i + 1) % n
        

    def save_uploaded_block(self, file_id, block_id, client_sock, replication):
        
        res = client_sock.recv(4086).decode()

        filename = "block" + str(block_id) + ".txt"
        dirname = "datanode" + str(self.dn_id) + "/file" + str(file_id)
        Path(dirname).mkdir(exist_ok=True)
        path = dirname + "/" + filename


        try:
            fp = open(path, "w")
            fp.write(res)
            if replication:
                print("\nsaved fileid ", file_id, " block_id ", block_id)
            status = 1
        except:
            status = 0

        client_sock.send(str(status).encode())
        client_sock.close()

        self.update_metadata(file_id, block_id)
        

        if replication:
        
            self.get_active_datanodes()
            
            print("About to replicate ", file_id, "_", block_id, sep = "")
            print("Active  datanodes: ", self.active_datanodes)
            self.replicate(res, file_id, block_id)
            print("Done")
            print("\n")

        


    def make_copy(self, src_id, new_id):
        src_dir = "datanode" + str(self.dn_id) + "/file" + src_id
        new_dir = "datanode" + str(self.dn_id) + "/file" + new_id
        if Path(src_dir).is_dir():
            shutil.copytree(src_dir, new_dir)

    def delete_file(self, file_id):
        dir_name = "datanode" + str(self.dn_id) + "/file" + file_id
        if Path(dir_name).exists() and Path(dir_name).is_dir():
            shutil.rmtree(dir_name)



def main():
    dn_id = sys.argv[1]
    dn = Datanode(dn_id)
    print(f"datanode {dn_id} up at port number {dn.port}")

    pool.submit(dn.ping_ack)                                                # assign thread for acknowledging namenode pings

    dn.socket.listen(30)
    while True:
        client_sock, addr = dn.socket.accept()
        
        action = client_sock.recv(1024).decode().split(" ")

        if action[0] == "upload":
            pool.submit(dn.save_uploaded_block, action[1], action[2], client_sock, True)

        if action[0] == "save":
            pool.submit(dn.save_uploaded_block, action[1], action[2], client_sock, False)

        if action[0] == "copy":
            pool.submit(dn.make_copy, action[1], action[2])
            client_sock.close()

        if action[0] == "delete":
            pool.submit(dn.delete_file, action[1])
            client_sock.close()
        ## lakshya 
        if action[0] == "GET_BLOCK_BY_ID":
            pool.submit(dn.send_block_by_id(action[2], action[1], client_sock))
            client_sock.close()
        if action[0] == "stream_block":
            pool.submit(dn.stream_block_by_id_line_by_line(action[1], action[2], client_sock))
            client_sock.close()

        
            
            

pool = concurrent.futures.ThreadPoolExecutor(max_workers=20)             # thread pool

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pool.shutdown(wait = False, cancel_futures=True)
        print("done, datanode quitting.")
        exit(0)









