import socket
import concurrent.futures
import time

from metdata_update import *
from namespace import *


class Namenode:
    def __init__(self):

        # keeps track of active DNs
        self.connections = []

        # server side TCP socket for listening to client connection requests
        self.port = 5000
        self.ip = socket.gethostname()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip, self.port))

        # SQL server info
        self.host = "localhost"
        self.user = "root"
        self.password = "Malay@2002@"
        self.database = "bdproject"

    def connect_to_MySQL(self):
        # connect to mysql server for metadata and namespace ops

        db = mysql.connector.connect(
            host = self.host,
            user = self.user,
            password = self.password,
            database = self.database
        )

        if db.is_connected():
            print("Connected to MySQL database")
            cursor = db.cursor()

        return cursor, db
    

    def fetch_active_datanodes(self):
        return self.connections

   

    def client(self, comm_socket, port):
        # communicates with client

        while True:
            data = comm_socket.recv(1024).decode()
            if data.strip().lower() == "bye":
                break
            
            command = data.strip().split(" ")

            if command[0] == "active":
                res = str(self.fetch_active_datanodes())
                comm_socket.send(res.encode())

            elif command[0] == "put":
                cursor, db = self.connect_to_MySQL()
                file_id = add_file_metadata_and_return_file_id(cursor, db, command[1], command[2], command[3])
                db.close()
                res = str(file_id)
                comm_socket.send(res.encode())
    

            elif command[0] == "metadata_update":
                cursor, db = self.connect_to_MySQL()
                status = add_block_metadata(cursor, db, command[1], command[2], command[3])
                db.close()
                comm_socket.send(str(status).encode())


            elif command[0] == "get_metadata":
                cursor, db = self.connect_to_MySQL()
                block_locations = get_datanodes_and_blocks(cursor, db, command[1])
                db.close()
                comm_socket.send(str(block_locations).encode())


            elif command[0] == "mkdir":
                cursor, db = self.connect_to_MySQL()
                status = create_directory(cursor, db, command[1])
                db.close()
                comm_socket.send(str(status).encode())

            elif command[0] == "rmdir":
                cursor, db = self.connect_to_MySQL()
                status = delete_directory(cursor, db, command[1])
                db.close()
                comm_socket.send(str(status).encode())

            elif command[0] == "rm":
                cursor, db = self.connect_to_MySQL()
                status = delete_file(cursor, db, command[1])
                db.close()
                comm_socket.send(str(status).encode())

            elif command[0] == "mvdir":
                cursor, db = self.connect_to_MySQL()
                status = move_directory(cursor, db, command[1], command[2])
                db.close()
                comm_socket.send(str(status).encode())

            elif command[0] == "mv":
                cursor, db = self.connect_to_MySQL()
                status = move_file(cursor, db, command[1], command[2])
                db.close()
                comm_socket.send(str(status).encode())

            elif command[0] == "cpdir":
                cursor, db = self.connect_to_MySQL()
                status = copy_directory(cursor, db, command[1], command[2])
                db.close()
                comm_socket.send(str(status).encode())
                

            elif command[0] == "cp":
                cursor, db = self.connect_to_MySQL()
                status = copy_file(cursor, db, command[1], command[2])
                db.close()
                comm_socket.send(str(status).encode())

            elif command[0] == "ls":
                print("in ls")
                cursor, db = self.connect_to_MySQL()
                filelist = list_files(cursor, db, command[1])
                print("got list, about to send:", filelist)
                db.close()
                comm_socket.send(str(filelist).encode())

            elif command[0] == "tree":
                cursor, db = self.connect_to_MySQL()
                tree = view_hierarchy(cursor, 1)
                db.close()
                comm_socket.send(str(tree).encode())
            elif command[0] == "GET_METADATA_BY_ID":
                cursor, db = self.connect_to_MySQL()
                block_datanode_dict = get_datanodes_and_blocks(cursor, db, command[1])
                db.close()
                comm_socket.send(str(block_datanode_dict).encode())
            elif command[0] == "download" or command[0] == "read":
                cursor, db = self.connect_to_MySQL()
                f_id = child_id(cursor, db, command[1])
                db.close()
                comm_socket.send(str(f_id).encode())
                
        comm_socket.close()



    def ping_datanode(self, comm_socket, port, dn_id):
        # pings datanode at an interval of 5 seconds


        print(f"Connected to datanode {dn_id}, on " + port)
        comm_socket.settimeout(3.0)

        while True:
            time.sleep(5)
            comm_socket.send("ping".encode())
            comm_socket.settimeout(3.0)
            data = comm_socket.recv(1024).decode()
            if not data:
                print(f"datanode {dn_id} did not acknowledge ping")
                break

        # if connection lost, update list of active datanodes
        self.connections.remove(int(dn_id))
        comm_socket.close()



def main():

    nn = Namenode()
    print(f"namenode up at port number {nn.port}")
    nn.socket.listen(20)

    while True:
        
        comm_socket, port = nn.socket.accept()                                  # accept connection request
        data = comm_socket.recv(1024).decode()

        #identify whether client or datanode
        if data == "client":
            pool.submit(nn.client, comm_socket, str(port))

        elif data.startswith("datanode"):
            pool.submit(nn.ping_datanode, comm_socket, str(port), data[-1])
            nn.connections.append(int(data[-1]))
            
        

pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)                # thread pool

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pool.shutdown(wait = False, cancel_futures=True)
        print("done, namenode quitting.")
        exit(0)


    

    





# add a timeout for the pinging between namenode and datanode



#for copy:
'''
    the metadata func should return file_id on copying a single file, or the
    list of file_ids copied on copying a directory (including files in subdirectories)
    then, establish a new comms path between nn and all dns, telling them to duplicate the
    directories pertaining to the file_id(s)
'''