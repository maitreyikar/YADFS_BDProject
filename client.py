import socket
import os
import random
import time
import ast

class Client:

    def __init__(self):
        self.namenode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)   # socket for communicating with namenode 
        self.block_size = 16
        self.active_datanodes = None

    def connect_to_datanode(self, dn_id):
        datanode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        datanode_socket.connect((socket.gethostname(), 3000 + dn_id))
        return datanode_socket
    
    def get_metadata(self, dfs_filepath):
        # returns active dn_ids which have blocks of the file
        pass
    
    def get_active_datanodes(self):
        # gets active datanodes from namenode

        self.namenode_socket.send("active datanodes".encode())
        slist = self.namenode_socket.recv(1024).decode()
        self.active_datanodes = slist[1:-1].split(", ")
        self.active_datanodes = list(map(int, self.active_datanodes))
        
    
    
    def send_block(self, file_id, block, block_id, dn_id):
        # given a block, sends it to the datanode 

        sock = self.connect_to_datanode(dn_id)
        attempts = 0
        
        # initalizing communication
        init_msg = f"upload {file_id} {block_id}"
        print("msg to dn: ", init_msg)
        sock.send(init_msg.encode())

        time.sleep(1)

        sock.send(block.encode())
        status = 0
        status = int(sock.recv(1024).decode())
        print("status: ", status)
        
        # update block metadata
        if status:
            self.namenode_socket.send(f"metadata_update {file_id} {block_id} {dn_id}".encode())
            res = self.namenode_socket.recv(1024).decode()
            # if res == "1":
            #     print("Updated metadata successfully.")
            # else:
            #     print("Could not update metadata")
        print("\n")

        # retry uploads of blocks which failed
        while not status and attempts < 2:
            print("resending block no. ", block_id)
            try:
                sock.send(block.encode())
                status = int(sock.recv(1024).decode())
            except ConnectionResetError:
                print(f"Datanode {dn_id} inactive")
                return 0
            attempts += 1

        time.sleep(1)

        sock.close()
        return status
        
    
    def form_blocks(self, local_filepath):
        # given a file as a string, creates blocks based on number of lines

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
    

    def validate_filepath(self, local_filepath, dfs_filepath):
        # requests namenode to update file metadata and namespace

        filename = local_filepath.split("/")[-1]
        finalpath = dfs_filepath + "/" + filename
        filesize = os.path.getsize(local_filepath)
        request = f"put {finalpath} {filesize}"
        self.namenode_socket.send(request.encode())

        response = self.namenode_socket.recv(1024).decode()
        response = response.split(" ")
        return int(response[0]), response[1]
    

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
      


    def retrieve_file_metadata_by_id(self, f_id):
        self.namenode_socket.send(f"get_metadata {f_id}".encode())
        response_dict = self.namenode_socket.recv(1024).decode()
        response = ast.literal_eval(response_dict)
        return response if response else None 
    
    
    def download_file(self, file_id, save = True):

        # Client Request: Requesting metadata for the file from the Namenode based on file ID
        metadata = self.retrieve_file_metadata_by_id(file_id)
        if not metadata:
            print("File metadata not found for file ID:", file_id)
            return

        blocks = []

        # Get a list of active datanodes
        self.get_active_datanodes()

        # Get each block from the datanodes
        for block_id in metadata:
            
            valid_dn = [int(i) for i in metadata[block_id] if int(i) in self.active_datanodes]
            
            dn_id = random.choice(valid_dn)
            
            temp = self.connect_to_datanode(dn_id)
            temp.send(f"GET_BLOCK_BY_ID {block_id} {file_id}".encode())
            block = temp.recv(4096).decode()
            if block != "Block not found":
                blocks.append(block)
            else:
                print(f"Error in fetching block {block_id}")
            temp.close()


        # While reassembly, either it gets printed to terminal or saved as a file
        if save:
            file_path = f"downloaded_{file_id}.txt"  # Assuming a generic file name for the downloaded file
            with open(file_path, 'w') as file:
                for block in blocks:
                    file.write(block)
            print("File downloaded successfully. Saved as:", file_path)

        else:
            for block in blocks:
                print(block)

    

    def write_to_file(self, local_filepath, dfs_filepath):
        pass


        
def main():
    cl = Client()
    try:
        cl.namenode_socket.connect((socket.gethostname(), 5000))
        cl.namenode_socket.send("client".encode())                               # self identifying inital message

    except ConnectionRefusedError:
        print("Namenode is not accepting connections.")
        exit(0)

    except Exception:
        print("Something went wrong. Restart client and snamenode.")
        exit(0)



    
    while True:
        print("\n")

        # get input from user
        action = input(" -> ")
        if action.strip().lower() == "bye":
            cl.namenode_socket.send(action.encode())
            break

        message = action.strip().split(" ")


        if message[0] == "upload":
            size = os.path.getsize(message[1])
            file_name = message[1].split("/")[-1]
            
            cl.namenode_socket.send(f"put {message[2]} {file_name} {size}".encode())

            file_id = int(cl.namenode_socket.recv(1024).decode())

            if file_id == -1:
                print("Invalid dfs path, try again\n")
            elif file_id == -2:
                print("A file of the same name already exists at the specified dfs path\n")
            elif file_id == -3:
                print("An exception occured")
            else:
                cl.upload_file(message[1], file_id)
                print("Uploaded file successfully\n")



        elif message[0] == "active":    
            cl.get_active_datanodes()
            print(cl.active_datanodes)



        elif message[0] == "mkdir":
            cl.namenode_socket.send(action.encode())
            res = int(cl.namenode_socket.recv(1024).decode())

            if res == 1:
                print("Directory created succesfully")
            else:
                print("Invalid DFS path, try again")
        


        elif message[0] == "rmdir":
            cl.namenode_socket.send(action.encode())
            res = cl.namenode_socket.recv(1024).decode()
            
            if res == "-1":
                print("Invalid DFS path, try again")
            else:
                print("Directory deleted successfully")
                if res == "[]":
                    continue
                res = res[1:-1].split(", ")
                print("file_ids to delete by rmdir:",res)
                cl.get_active_datanodes()

                #delete blocks from datanodes
                for file in res:
                    for dn_id in cl.active_datanodes:
                        sock = cl.connect_to_datanode(dn_id)
                        sock.send(f"delete {file}".encode())
                        sock.close()



        elif message[0] == "rm":
            cl.namenode_socket.send(action.encode())
            res = cl.namenode_socket.recv(1024).decode()

            if res == "-1":
                print("Invalid DFS path, try again")
            else:
                print("File deleted successfully")
                cl.get_active_datanodes()

                #delete blocks form datanodes
                for dn_id in cl.active_datanodes:
                    sock = cl.connect_to_datanode(dn_id)
                    sock.send(f"delete {res}".encode())
                    sock.close()



        elif message[0] == "mvdir":
            cl.namenode_socket.send(action.encode())
            res = int(cl.namenode_socket.recv(1024).decode())

            if res == 1:
                print("Directory moved succesfully")
            elif res == -1:
                print("Invalid source path, try again")
            elif res == -2:
                print("Invalid destination path, try again")
            elif res == -3:
                print("A directory of the same name already exists in the destination directory")
            

        elif message[0] == "mv":
            cl.namenode_socket.send(action.encode())
            res = int(cl.namenode_socket.recv(1024).decode())

            if res == 1:
                print("File moved succesfully")
            elif res == -1:
                print("Invalid source path, try again")
            elif res == -2:
                print("Invalid destination path, try again")
            elif res == -3:
                print("A file of the same name already exists in the destination directory")



        elif message[0] == "cpdir":
            cl.namenode_socket.send(action.encode())
            file_id_update = cl.namenode_socket.recv(1024).decode()
            
            if file_id_update == "-1":
                print("Invalid source path, try again")
            elif file_id_update == "-2":
                print("Invalid destination path, try again")
            elif file_id_update == "-3":
                print("A directory of the same name already exists in the destination directory")
            else:
                print("Directory copied succesfully")
                if file_id_update == "[]":
                    continue

                # src_file_id, copied_file_id as a result of cpdir
                file_id_update = file_id_update[2:-2].split("), (")
                file_id_update = [i.split(", ") for i in file_id_update]
                
                cl.get_active_datanodes()

                # making copies in DNs
                for file in file_id_update:
                    for dn_id in cl.active_datanodes:
                        sock = cl.connect_to_datanode(dn_id)
                        sock.send(f"copy {file[0]} {file[1]}".encode())
                        sock.close()

            

        elif message[0] == "cp":
            cl.namenode_socket.send(action.encode())
            file_id_update = cl.namenode_socket.recv(1024).decode()
            
            if file_id_update == "-1":
                print("Invalid source path, try again")
            elif file_id_update == "-2":
                print("Invalid destination path, try again")
            elif file_id_update == "-3":
                print("A file of the same name already exists in the destination directory")
            else:
                print("File copied successfully")

                # src_file_id, new_file_id 
                file_id_update = file_id_update[2:-2].split("), (")
                file_id_update = [i.split(", ") for i in file_id_update]

                cl.get_active_datanodes()
                for dn_id in cl.active_datanodes:
                    sock = cl.connect_to_datanode(dn_id)
                    sock.send(f"copy {file_id_update[0][0]} {file_id_update[0][1]}".encode())
                    sock.close()



        elif message[0] == "ls":
            cl.namenode_socket.send(action.encode())
            res = cl.namenode_socket.recv(1024).decode()
            if res == "-1":
                print("Invalid DFS path, try again")
            else:
                print("\nNAME".ljust(30), "TIME OF CREATION".ljust(29), "SIZE (BYTES)".ljust(30))
                res = res[1:-1].split(", ")
                for i in res:
                    cols = i[1:-1].split(" ")
                    for c in cols:
                        print(c.ljust(30), end = "")
                    print("")



        elif message[0] == "tree":
            cl.namenode_socket.send(action.encode())
            res = cl.namenode_socket.recv(1024).decode()
            
            if res == "-1":
                print("An error occured, try again")
            else:
                print("")
                res = res[1:-1].split(", ")
                for i in res:
                    print(i[1:-1])
                    
        elif message[0] == "download":
            cl.namenode_socket.send(action.encode())
            res = cl.namenode_socket.recv(1024).decode()
            res2 = int(res)
            cl.download_file(res2)  
               
        elif message[0] == "read":         
            cl.namenode_socket.send(action.encode())
            res = cl.namenode_socket.recv(1024).decode() 
            res2 = int(res)
            cl.download_file(res2, save = False)    

    cl.namenode_socket.close()                                               # close the connection
    print("done, client quitting.")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("done, client quitting.")
        exit(0)