import mysql.connector
import socket
import enum
import random
from typing import Dict, List
import struct
import os
import json
import jsonschema

def to_hex_string(byte_array: bytearray):
    return ''.join(format(byte, '02x') for byte in byte_array)

def generate_rand_bytes():
    return os.urandom(3) + b'\x00'

class RequestType(enum.Enum):
    CREATE = 0x01
    READ = 0x02
    WRITE = 0x03
    REMOVE = 0X04
    FAIL = 0x05

class DBManager:
    db_conn = None
    db_cursor = None
    def __init__(self):
        try:
            self.db_conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='pass',
                database='fsmap'
            )
            self.db_cursor = self.db_conn.cursor()
            print("Successfully connected to MySQL")
        except mysql.connector.Error as e:
            raise Exception("Error connecting to MySQL: " + str(e))

    def insert_filemapping(self, filename, node) -> bool:
        query = "INSERT INTO file_to_node (filename, node) VALUES (%s, %s)"
        try:
            self.db_cursor.execute(query, (filename, node))
            self.db_conn.commit()
        except mysql.connector.Error as e:
            print("Could not insert file mapping. Error " + str(e))
            return False
        return True
    
    def get_nodefromfile(self, filename) -> int:
        query = "SELECT * FROM file_to_node WHERE filename=%s"
        try:
            self.db_cursor.execute(query, (filename,))
            response = self.db_cursor.fetchone()
            if response != None:
                return int(response[1])
        except mysql.connector.Error as e:
            print("Could not get node from file. Error " + str(e))
    
    def get_allmappings(self):
        query = "SELECT * FROM file_to_node"
        try:
            self.db_cursor.execute(query)
        except mysql.connector.Error as e:
            print("Could not get all mappings. Error " + str(e))
        return self.db_cursor.fetchall()
    
    def get_metanamefromfile(self, filename):
        query = "SELECT * FROM file_to_metadata WHERE filename=%s"
        try:
            self.db_cursor.execute(query, (filename,))
            response = self.db_cursor.fetchone()
            if response != None:
                return response[1]
        except mysql.connector.Error as e:
            print("Could not get metadata file. Error " + str(e))
    
    def insert_metamapping(self, filename, metadata) -> bool:
        query = "INSERT INTO file_to_metadata (filename, metadata) VALUES (%s, %s)"
        try:
            self.db_cursor.execute(query, (filename, metadata))
            self.db_conn.commit()
        except mysql.connector.Error as e:
            if isinstance(e, mysql.connector.IntegrityError):
                print("Could not insert file mapping. Error " + str(e))
            return False
        return True

class MessageBroker:
    socket_map: Dict[int, socket.socket] = {}
    def __init__(self, ports):
        for port in ports:
            try:
                new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                new_socket.connect(('localhost', port))
                self.socket_map[port] = new_socket
                print("Successfully connected to port: " + str(port))
            except ConnectionRefusedError:
                print("Connection refused. The server may not be running or the address/port is incorrect.")
            except socket.error as err:
                print(f"Socket error occurred: {err}")
    def send(self, port, data):
        if port not in self.socket_map.keys():
            return b''
        with self.socket_map[port] as client_socket:
            client_socket.sendall(data)
            response = client_socket.recv(4096)
            client_socket.close()
            new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            new_socket.connect(('localhost', port))
            self.socket_map[port] = new_socket
            return response

metadata_default = {
    "filename": None,
    "num_chunks": 0,
    "chunks": {}
}
metadata_schema = {
    "type": "object",
    "properties": {
        "filename": {"type": "string"},
        "num_chunks": {"type": "integer", "minimum": 0},
        "chunks": {
            "type": "object",
            "additionalProperties": {
                "type": "string"
            }
        }
    },
    "required": ["filename", "num_chunks", "chunks"]
}

class ChunkMapper:
    # file->metadata filename from db
    # metadata -> chunk names + nodes
    db_manager: DBManager = None
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager
    def _metadata_format_valid(self, metadata) -> bool:
        try:
            jsonschema.validate(metadata, metadata_schema)
            return True
        except jsonschema.exceptions.ValidationError as e:
            return False
    def get_metadata(self, filename) -> List:
        metaname = self.db_manager.get_metanamefromfile(filename)
        if metaname == None:
            return
        metaname_str = to_hex_string(metaname)
        metadata = None
        with open(metaname_str, "r") as json_file:
            metadata = json.load(json_file)
            return [metadata, metaname]
    def get_chunks(self, filename) -> Dict:
        metadata, = self.get_metadata(filename)
        if metadata == None:
            return
        return metadata['chunks']
    # returns name of the new chunk in bytes
    def insert_chunk(self, filename, node_id) -> bytearray:
        metadata, metaname = self.get_metadata(filename)
        if not self._metadata_format_valid(metadata):
            return
        metadata["num_chunks"] += 1
        new_id = metadata["num_chunks"]
        metadata["chunks"][new_id] = node_id
        json_string = json.dumps(metadata)
        metaname_str = to_hex_string(metaname)
        with open(metaname_str, "w") as json_file:
            json_file.write(json_string)
        metaname[3] = new_id
        return metaname

    def create_metafile(self, filename) -> None:
        metaname = self.db_manager.get_metanamefromfile(filename)
        if metaname:
            print(filename + " already has a metadata file mapping")
            return
        success = False
        while not success:
            metaname = generate_rand_bytes()
            success = self.db_manager.insert_metamapping(filename, metaname)
        metaname = to_hex_string(metaname)
        data = metadata_schema
        data["filename"] = filename
        json_string = json.dumps(data)
        with open(metaname, "w") as json_file:
            json_file.write(json_string)

class MessageReceiver:
    host = None
    port = None
    bind_socket: socket.socket = None
    msg_broker: MessageBroker = None
    db_manager: DBManager = None
    def __init__(self, host, port, msg_broker: MessageBroker, db_manager: DBManager):
        self.msg_broker = msg_broker
        self.db_manager = db_manager
        self.host = host
        self.port = port
        self.bind_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bind_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    def start(self):
        self.bind_socket.bind((self.host, self.port))
        self.bind_socket.listen(1)
        print(f"Server listening on {self.host}:{self.port}...")
        while True:
            conn, addr = self.bind_socket.accept()
            print(f"Connection established from {addr}")
            self.handle_connection(conn)
    
    def handle_connection(self, conn):
        data = conn.recv(1024)
        print(f"Received data: {data}")
        req_type = data[0]
        fh_len = data[1]
        fh = data[2:2+fh_len].decode()
        worker_port = None
        response = struct.pack('B', RequestType.FAIL.value)
        match req_type:
            case RequestType.CREATE.value:
                worker_port = random.choice(list(self.msg_broker.socket_map.keys()))
                worker_port = 1235
                success = self.db_manager.insert_filemapping(fh, worker_port)
                if not success:
                    print("Could not insert filemapping into database.")
                    conn.sendall(response)
                    return
            case _:
                worker_port = self.db_manager.get_nodefromfile(fh)
                if worker_port == None:
                    conn.sendall(response)
                    return
        response = self.msg_broker.send(worker_port, data)
        conn.sendall(response)

if __name__ == "__main__":
    host = "127.0.0.1"
    port = 1234
    db_manager = DBManager()
    chunk_mapper = ChunkMapper(db_manager)
    print(chunk_mapper.insert_chunk("Tst", "node"))

    # worker_ports = [1235, 1236, 1237]
    # # keeps a handle on all the worker ports and forwards msg back and forth
    # msg_broker = MessageBroker(worker_ports)
    # # receives messages from clients and handles them
    # msg_recv = MessageReceiver(host, port, msg_broker, db_manager)
    # msg_recv.start()