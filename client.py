import socket


def client_program():
    host = socket.gethostname()
    port = 5000  # socket server port number

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # instantiate
    client_socket.connect((host, port))  # connect to the server

    message = "client"

    while message.lower().strip() != 'bye':
        client_socket.send(message.encode())  # send message
        message = input(" -> ")  # again take input

    client_socket.close()  # close the connection



if __name__ == '__main__':
    client_program()