import socket
import threading
import concurrent.futures

# is the datanode a client or a server? or both? so two functions?

class Namenode:
    def __init__(self):
        self.clients = {}
        self.port = 5000
        self.host = socket.gethostname()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        #self.socket.listen(5)
    
    # # maybe not required
    # def get_client_type(self):
    #     # when a new connection request arrives, a thread will execute this
    #     # it listens to the first message which tells it what kind of client it is
    #     # store it in self.clients, return it too
    #     pass

    # #maybe not required
    # def listen(self):
    #     # listens for new connection requests
    #     # creates threads to handle each?
    #     pass

    def ping(self):
        # pings the data nodes periodically
        pass

    def fetch_metadata(self, filename):
        # returns file block locations from database
        pass

    def update_metadata(self, filename):
        # addition/deletion/update of metadata
        pass

    def client(self, comm_socket, port):
        # talks to client
        print("Talking to client, on port: " + str(port))

        while True:
            data = comm_socket.recv(1024).decode()
            if not data:
                break
            print("from connected user: " + str(data))
        comm_socket.close()

    def datanode(self, comm_socket, port, dn_id):
        # talks to datanode
        print("Talking to dn!")

pool = concurrent.futures.ThreadPoolExecutor(max_workers=5)

nn = Namenode()
nn.socket.listen(5)



while True:
    comm_socket, port = nn.socket.accept()
    data = str(comm_socket.recv(1024).decode())
    
    if data == "client":
        pool.submit(nn.client, comm_socket, port)
    elif data.startswith("datanode"):
        pool.submit(nn.datanode, comm_socket, port, data[-1])

    

    





