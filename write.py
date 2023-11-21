import socket
import os
import random
import time
import json
import sys
import concurrent.futures
import shutil
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

    def delete_file(self, file_id):
        dir_name = "datanode" + str(self.dn_id) + "/file" + file_id
        if Path(dir_name).exists() and Path(dir_name).is_dir():
            shutil.rmtree(dir_name)

    def update_metadata(self, file_id, block_id):
        temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp.connect((self.host, 5000))
        temp.send("client".encode())
        time.sleep(2)
        temp.send(f"metadata_update {file_id} {block_id} {self.dn_id}".encode())
        res = int(temp.recv(1024).decode())
        temp.send("bye".encode())
        temp.close()


class Client:
    def __init__(self):
        self.namenode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)   # socket for communicating with namenode 
        self.namenode_socket.connect((socket.gethostname(), 5000))                 # Assuming Namenode is running on port 5000
        self.block_size = 16                                                        # Adjust this as per your configuration
        self.active_datanodes = None

    def connect_to_datanode(self, dn_id):
        datanode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        datanode_socket.connect((socket.gethostname(), 3000 + dn_id))              # Assuming Datanodes are running on ports 3000 + dn_id
        return datanode_socket

############# modified
    def get_metadata_from_namenode(self, filename):
        self.namenode_socket.send(f"GET_METADATA {filename}".encode())
        metadata = self.namenode_socket.recv(64000).decode() #64kb
        return json.loads(metadata)
    
    def read_file(self, filename):
        metadata = self.get_metadata_from_namenode(filename)
        file_blocks = []
        for block_info in metadata['blocks']:
            datanode_id = block_info['datanode_id']
            block_id = block_info['block_id']
            datanode_socket = self.connect_to_datanode(datanode_id)
            datanode_socket.send(f"READ {filename} {block_id}".encode())
            block_data = datanode_socket.recv(self.block_size)
            file_blocks.append(block_data)
            datanode_socket.close()
        file_content = b''.join(file_blocks)
        print(file_content.decode())

    def form_blocks(self, local_filepath):
        fp = open(local_filepath, "r")
        
        block = ""
        file_blocks = []
        no_of_lines_read = 0

        while True:
            line = fp.readline()

            if not line:
                break

            block += line
            no_of_lines_read += 1

            if no_of_lines_read == self.block_size:
                file_blocks.append(block)
                block = ""
                no_of_lines_read = 0
        
        if block != "":
            file_blocks.append(block)

        return file_blocks
    
    def upload_file(self, local_filepath, file_id):
       
        # get a list of active datanodes
        self.get_active_datanodes()

        # create blocks
        file_blocks = self.form_blocks(local_filepath)
        print(len(file_blocks))

        # choose first DN randomly
        n = len(self.active_datanodes)
        i = random.randint(0,n-1)

        # send blocks in a round robin manner
        for b in range(len(file_blocks)):
            dn_id = self.active_datanodes[i]
            print("current dn_id: ", dn_id)
            status = self.send_block(file_id, file_blocks[b], b, dn_id)
            
            if not status:
                print(f"datanode {dn_id} not responding")
                self.active_datanodes.remove(dn_id)
                n = len(self.active_datanodes)
                i -= 1
                b -= 1


            i  = (i + 1) % n

    def write_file(self, filepath):
        file_blocks = self.form_blocks(self,filepath)
        metadata=self.get_metadata_from_namenode(filepath)
        if metadata:
            #deleting if file exists
            for block_id in metadata:
                datanode_id = metadata[block_id][0]
                datanode_socket = self.connect_to_datanode(datanode_id)
                datanode_socket.send(f"delete {block_id}".encode())
        
        #update metadata, get fileid and update timestamp
        #update timestamp -- it will give file id as well
        fileid=self.namenode_socket.send(f"getfileid {filepath}".encode())

        #delete the block
        self.namenode_socket.send(f"rmblock {fileid}".encode())
        
        #creating directory, inserting blocks and updating metadata
        datanode_socket = self.connect_to_datanode(datanode_id)
        datanode_socket.send(f"upload {fileid} {block_id}".encode())

        datanode_socket.close()

client=Client()
client.write_file("sample.txt", 1)

