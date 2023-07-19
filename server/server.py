import mysql.connector
import socket
from constants import *

class DBManager:
    host='localhost'
    user='root'
    password='pass'
    database='fsmap'
    conn = None
    cursor = None
    def __init__(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.conn.cursor()
            print("Successfully connected to MySQL")
        except mysql.connector.Error as e:
            raise Exception("Error connecting to MySQL: " + str(e))
    def __del__(self):
        
        self.cursor.close()
        self.conn.close()
    
    def 
# cursor.execute("INSERT INTO file_to_node (filename, node) VALUES (%s, %s)", ("test", "nodetest"))

class RequestHandler:
    socket = None
    host = None
    port = None
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    def start(self):
        self.socket.bind((self.host, self.port))
        self.socket.listen(1)
        print(f"Server listening on {self.host}:{self.port}...")

        while True:
            conn, addr = self.socket.accept()
            print(f"Connection established from {addr}")
            self.handle_connection(conn)
    
    def handle_connection(self, conn):
        data = conn.recv(1024)
        print(f"Received data {data}")

if __name__ == "__main__":
    db_manager = DBManager()