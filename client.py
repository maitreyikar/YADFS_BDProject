import socket
import os
import random
import time
import json

class Client:

    def retrieve_block(self, datanode_socket, file_id, block_id):
        datanode_socket.send(f"get_block {file_id} {block_id}".encode())
        block_data = datanode_socket.recv(self.block_size)  # Adjust block size accordingly
        datanode_socket.close()
        return block_data

    def stream_block(self, datanode_socket, file_id, block_id):
        datanode_socket.send(f"stream_block {file_id} {block_id}".encode())
        while True:
            line = datanode_socket.recv(self.block_size)  # Adjust block size accordingly
            if not line:
                break
            yield line.decode('utf-8')
            datanode_socket.send(b'ACK')  # Sending acknowledgment for the next line

        def __init__(self):
            self.namenode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)   # socket for communicating with namenode 
            self.block_size = 16
            self.active_datanodes = None

    def connect_to_datanode(self, dn_id):
        datanode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        datanode_socket.connect((socket.gethostname(), 3000 + dn_id))
        return datanode_socket
    
    def get_metadata(self, dfs_filepath):
        #returns active dn_ids which have blocks of the file
        pass
    
    def get_active_datanodes(self):
        self.namenode_socket.send("active datanodes".encode())
        slist = self.namenode_socket.recv(1024).decode()
        self.active_datanodes = slist[1:-1].split(", ")
        self.active_datanodes = list(map(int, self.active_datanodes))
        
    
    
    def send_block(self, file_id, block, block_id, dn_id):
        sock = self.connect_to_datanode(dn_id)
        attempts = 0
        
        init_msg = f"upload {file_id} {block_id}"
        print("msg to dn: ", init_msg)
        sock.send(init_msg.encode())

        time.sleep(1)

        sock.send(block.encode())
        status = 0
        status = int(sock.recv(1024).decode())
        print("status: ", status)
        

        if status:
            self.namenode_socket.send(f"metadata_update {file_id} {block_id} {dn_id}".encode())
            res = int(self.namenode_socket.recv(1024).decode())
            if res == 1:
                print("Updated metadata successfully.")
            else:
                print("Could not update metadata")
        print("\n")

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
        filename = local_filepath.split("/")[-1]
        finalpath = dfs_filepath + "/" + filename
        filesize = os.path.getsize(local_filepath)
        request = f"put {finalpath} {filesize}"
        self.namenode_socket.send(request.encode())

        response = self.namenode_socket.recv(1024).decode()
        response = response.split(" ")
        return int(response[0]), response[1]
    

    def upload_file(self, local_filepath, file_id):
       
        self.get_active_datanodes()


        file_blocks = self.form_blocks(local_filepath)
        print(len(file_blocks))

        n = len(self.active_datanodes)
        i = random.randint(0,n-1)

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
                
######################################################################################################




# lakshya - reassemble block wise -- sequentially  (no blocks appear out of order)
    def download_file_by_id(self, f_id):
        # 1. Client Request: Requesting metadata for the file from the Namenode based on file ID
        metadata = self.retrieve_file_metadata_by_id(f_id)
        if not metadata:
            print("File metadata not found for file ID:", f_id)
            return

        # 2. Block Location Retrieval
        block_locations = metadata['block_locations']

        # 3. Data Block Retrieval: Retrieving each data block in parallel
        '''
        This section uses a ThreadPoolExecutor to concurrently retrieve each data block associated with the file. 
        It creates a list of futures, where each future corresponds to the retrieval of a data block, and 
        then it collects the results from these futures into the blocks list.
        '''
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.retrieve_data_block_by_id, block_id, f_id) for block_id in block_locations]
            blocks = [future.result() for future in futures]

        # 4. Reassembly
        file_path = f"downloaded_{f_id}.dat"  # Assuming a generic file name for the downloaded file
        with open(file_path, 'wb') as file:
            for block in blocks:
                file.write(block)

        # 5. File Completion Check and Cleanup
        print("File downloaded successfully. Saved as:", file_path)

    # def retrieve_file_metadata_by_id(self, f_id):
    #     # Code to retrieve file metadata from the Namenode using file ID
    #     pass

    # def retrieve_data_block_by_id(self, block_id):
    #     # Code to retrieve a specific data block from the Data Node using block ID
    #     pass



## lakshya
## json.loads() for dictionary
    def retrieve_file_metadata_by_id(self, f_id):
        # Connect to the Namenode
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.namenode_host, self.namenode_port))
            # Send the request for file metadata
            s.sendall(f"GET_METADATA_BY_ID {f_id}".encode()) # name node gets the string and then parses
            # Wait for the response
            response_dict = s.recv(1024).decode()
            response = json.loads(response_dict)
            return eval(response) if response else None

    def retrieve_data_block_by_id(self, block_id, file_id):
        # Determine the Datanode's address and port from the block ID
        datanode_host, datanode_port = self.get_datanode_address(block_id)

        # Connect to the Datanode
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((datanode_host, datanode_port))
            # Send the request for the data block
            s.sendall(f"GET_BLOCK_BY_ID {block_id} {file_id}".encode())
            # Wait for the block data
            block_data = s.recv(self.block_size)
            return block_data

    def get_datanode_address(self, block_id):
        # Placeholder method to determine the Datanode's address from the block ID
        datanode_host = 'localhost'
        datanode_port = 3000 + (block_id % self.number_of_datanodes)  # Example logic
        return datanode_host, datanode_port
    

#################################################################################################
# # sort w map
# metadata - dict
# read in the right order (block_id) -- done
    def read_file_by_id_line_by_line(self, f_id):
        # Retrieve file metadata using file ID
        metadata = self.retrieve_file_metadata_by_id(f_id)
        if not metadata:
            print("File metadata not found for file ID:", f_id)
            return

        block_locations = metadata['block_locations']


        for block_id in block_locations:
            dn_id, block_id = block_locations[block_id]
            datanode_socket = self.connect_to_datanode(dn_id)
            datanode_socket.send(f"stream_block {f_id} {block_id}".encode())

            while True:
                line = datanode_socket.recv(self.block_size)  # Adjust block size accordingly
                if not line:
                    break
                yield line.decode('utf-8')
                datanode_socket.send(b'ACK')  # Sending acknowledgment for the next line

            datanode_socket.close()
                # Read and process each block
        for block_id in block_locations:
            block_data = self.retrieve_data_block_by_id(block_id)
            for line in block_data.splitlines():
                yield line.decode('utf-8')  # having assumed that the data is UTF-8 encoded

#################################################################################################
##  download file by id meta data req  ---- retrun dict , retreive data
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
                print("An error occured")
            

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
                print("An error occured")



        elif message[0] == "cpdir":
            cl.namenode_socket.send(action.encode())
            file_id_update = cl.namenode_socket.recv(1024).decode()
            
            if file_id_update == "-1":
                print("Invalid source path, try again")
            elif file_id_update == "-2":
                print("Invalid destination path, try again")
            elif file_id_update == "-3":
                print("An error occured")
            else:
                print("Directory copied succesfully")
                if file_id_update == "[]":
                    continue
                file_id_update = file_id_update[2:-2].split("), (")
                file_id_update = [i.split(", ") for i in file_id_update]
                #print(file_id_update)
                cl.get_active_datanodes()

                for file in file_id_update:
                    for dn_id in cl.active_datanodes:
                        sock = cl.connect_to_datanode(dn_id)
                        sock.send(f"copy {file[0]} {file[1]}".encode())
                        #time.sleep(1)
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
                file_id_update = file_id_update[2:-2].split("), (")
                file_id_update = [i.split(", ") for i in file_id_update]
                #print(file_id_update)
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

                    
        elif message[0] == "download":
            cl.download_file_by_id(message[1])
        
        elif message[0] == "read":
            cl.read_file_by_id_line_by_line(message[1])

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
        


        
        


            

        


    cl.namenode_socket.close()                                               # close the connection
    print("done, client quitting.")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("done, client quitting.")
        exit(0)




































