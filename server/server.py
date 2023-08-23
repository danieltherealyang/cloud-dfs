import mysql.connector
import socket
import enum
import random
from typing import Dict, List
import struct
import os
import json
import jsonschema
import time

chunk_limit = 5

def to_hex_string(byte_array: bytearray):
    return ''.join(format(byte, '02x') for byte in byte_array)

def generate_rand_bytes():
    return os.urandom(3) + b'\x00'

def generate_lock_id():
    return random.randint(0, (1<<32)-1)
def convert_dict_to_int(original_dict) -> Dict[int,int]:
    new_dict = {}
    for key, value in original_dict.items():
        new_key = int(key)
        new_value = int(value)
        new_dict[new_key] = new_value
    return new_dict

def encode_create_request(filename: bytes):
    operation_type = b'\x01'
    filename_length = struct.pack('!B', len(filename))
    filename_bytes = filename

    request = operation_type + filename_length + filename_bytes
    return request

def encode_read_request(file_handle: bytes, offset: int, num_bytes: int):
    operation_type = b'\x02'
    file_handle_length = struct.pack('!B', len(file_handle))
    file_handle_bytes = file_handle
    offset_bytes = struct.pack('!Q', offset)
    num_bytes_bytes = struct.pack('!I', num_bytes)

    request = operation_type + file_handle_length + file_handle_bytes + offset_bytes + num_bytes_bytes
    return request

def encode_write_request(file_handle: bytes, offset: int, data: bytes):
    operation_type = b'\x03'
    file_handle_length = struct.pack('!B', len(file_handle))
    file_handle_bytes = file_handle
    offset_bytes = struct.pack('!Q', offset)
    data_length = struct.pack('!B', len(data))
    data_bytes = data
    request = operation_type + file_handle_length + file_handle_bytes + offset_bytes + data_length + data_bytes
    return request

def encode_remove_request(file_handle: bytes):
    operation_type = b'\x04'
    filename_length = struct.pack('!B', len(file_handle))
    filename_bytes = file_handle

    request = operation_type + filename_length + filename_bytes
    return request

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
    
    def get_metanamefromfile(self, filename) -> bytearray:
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
    
    def rm_metafilefromfile(self, filename):
        query = "DELETE FROM file_to_metadata WHERE filename=%s"
        try:
            self.db_cursor.execute(query, (filename,))
            self.db_conn.commit()
        except mysql.connector.Error as e:
            print("Could not get metadata file. Error " + str(e))

#keeps track of locks and manages who has the lock
class LockService:
    #shared data across all instances, acts as "server state" for later migration
    lock_ids: set[int] = set()
    file_map: Dict[str, int] = {}
    id_map: Dict[int, str] = {}
    
    def register_client(self):
        id: int = generate_lock_id()
        while id in LockService.lock_ids:
            id = generate_lock_id()
        LockService.lock_ids.add(id)
        return id
    
    def remove_client(self, id):
        LockService.lock_ids.remove(id)
        filename = LockService.id_map.pop(id, None)
        if filename != None:
            LockService.file_map.pop(filename)
    
    def request_lock(self, id, filename) -> bool:
        if id not in LockService.lock_ids:
            print(f"{id} not in lock_ids")
            return False
        if id in LockService.id_map.keys():
            print(f"{id} already locking a file. Can only have on lock at a time")
            return False
        if filename in LockService.file_map.keys():
            print(f"{filename} is already locked by another lock_id")
            return False
        LockService.id_map[id] = filename
        LockService.file_map[filename] = id
        return True
    
    def release_lock(self, id, filename) -> bool:
        if id not in LockService.id_map.keys():
            print(f"{id} has no resource associated.")
            return False
        if filename not in LockService.file_map.keys():
            print(f"{filename} has no lock id associated.")
            return False
        LockService.id_map.pop(id)
        LockService.file_map.pop(filename)
        return True
    
    def locked(self, filename) -> bool:
        return filename in LockService.file_map.keys()
        
class LockClient:
    def __init__(self):
        self.lock_service = LockService()
        self.client_id = self.lock_service.register_client()
    
    def __del__(self):
        self.lock_service.remove_client(self.client_id)
    
    def lock(self, filename, timeout=3, interval=1) -> bool:
        while timeout>0:
            success = self.lock_service.request_lock(self.client_id, filename)
            if success:
                return True
            duration = min(timeout, interval)
            timeout -= duration
            time.sleep(duration)
        return False
    
    def release(self, filename):
        self.lock_service.release_lock(self.client_id, filename)

#keeps track of locks and manages who has the lock
class LockService:
    #shared data across all instances, acts as "server state" for later migration
    lock_ids: set[int] = set()
    file_map: Dict[str, int] = {}
    id_map: Dict[int, str] = {}
    
    def register_client(self):
        id: int = generate_lock_id()
        while id in LockService.lock_ids:
            id = generate_lock_id()
        LockService.lock_ids.add(id)
        return id
    
    def remove_client(self, id):
        LockService.lock_ids.remove(id)
        filename = LockService.id_map.pop(id, None)
        if filename != None:
            LockService.file_map.pop(filename)
    
    def request_lock(self, id, filename) -> bool:
        if id not in LockService.lock_ids:
            print(f"{id} not in lock_ids")
            return False
        if id in LockService.id_map.keys():
            print(f"{id} already locking a file. Can only have on lock at a time")
            return False
        if filename in LockService.file_map.keys():
            print(f"{filename} is already locked by another lock_id")
            return False
        LockService.id_map[id] = filename
        LockService.file_map[filename] = id
        return True
    
    def release_lock(self, id, filename) -> bool:
        if id not in LockService.id_map.keys():
            print(f"{id} has no resource associated.")
            return False
        if filename not in LockService.file_map.keys():
            print(f"{filename} has no lock id associated.")
            return False
        LockService.id_map.pop(id)
        LockService.file_map.pop(filename)
        return True
    
    def locked(self, filename) -> bool:
        return filename in LockService.file_map.keys()
        
class LockClient:
    def __init__(self):
        self.lock_service = LockService()
        self.client_id = self.lock_service.register_client()
    
    def __del__(self):
        self.lock_service.remove_client(self.client_id)
    
    def lock(self, filename, timeout=3, interval=1) -> bool:
        while timeout>0:
            success = self.lock_service.request_lock(self.client_id, filename)
            if success:
                return True
            duration = min(timeout, interval)
            timeout -= duration
            time.sleep(duration)
        return False
    
    def release(self, filename):
        self.lock_service.release_lock(self.client_id, filename)

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
                "type": "integer"
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
        try:
            with open(metaname_str, "r") as json_file:
                metadata = json.load(json_file)
            return [metadata, metaname]
        except FileNotFoundError:
            print(f"File {metaname_str} not found.")
        return
    def get_chunks(self, filename) -> Dict:
        metadata, _ = self.get_metadata(filename)
        if metadata == None:
            return
        return convert_dict_to_int(metadata['chunks'])
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
        data = metadata_default
        data["filename"] = filename
        json_string = json.dumps(data)
        with open(metaname, "w") as json_file:
            json_file.write(json_string)
    def remove_metafile(self, metaname) -> None:
        if not metaname:
            print(metaname + " does not exist, nothing to remove")
            return
        os.remove(metaname)

class MessageReceiver:
    host = None
    port = None
    bind_socket: socket.socket = None
    msg_broker: MessageBroker = None
    db_manager: DBManager = None
    chunk_mapper: ChunkMapper = None
    def __init__(self, host, port, msg_broker: MessageBroker, db_manager: DBManager, chunk_mapper: ChunkMapper):
        self.msg_broker = msg_broker
        self.db_manager = db_manager
        self.chunk_mapper = chunk_mapper
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
    
    def select_worker(self):
        worker_port = random.choice(list(self.msg_broker.socket_map.keys()))
        return worker_port
    
    def handle_connection(self, conn):
        data = conn.recv(1024)
        print(f"Received data: {data}")
        req_type = data[0]
        fh_len = data[1]
        fh = data[2:2+fh_len].decode()
        worker_port = None
        response = struct.pack('B', RequestType.FAIL.value)
        metaname = self.db_manager.get_metanamefromfile(fh)
        match req_type:
            case RequestType.CREATE.value:
                worker_port = self.select_worker()
                # worker_port = 1235
                # success = self.db_manager.insert_filemapping(fh, worker_port)
                # if not success:
                #     print("Could not insert filemapping into database.")
                #     conn.sendall(response)
                #     return
                # create metadata file, update to store node id, send msg.
                if metaname != None:
                    print(fh + " file already exists.")
                    conn.sendall(response)
                    return
                self.chunk_mapper.create_metafile(fh)
                response = struct.pack("!BI", 0x01, len(fh)) + fh.encode()
            case RequestType.REMOVE.value:
                if metaname == None:
                    print(fh + " does not exist.")
                    conn.sendall(response)
                    return
                chunks = self.chunk_mapper.get_chunks(fh)
                for chunk_id in chunks.keys():
                    chunk_name = bytes(to_hex_string(metaname[:3]+chunk_id.to_bytes(1, 'big')), "utf-8")
                    request = encode_remove_request(chunk_name)
                    self.msg_broker.send(chunks[chunk_id], request)
                self.chunk_mapper.remove_metafile(to_hex_string(metaname))
                self.db_manager.rm_metafilefromfile(fh)
                response = struct.pack("!BI", 0x04, 0x0)
            case RequestType.READ.value:
                if metaname == None:
                    print(f"{fh} does not exist")
                    conn.sendall(response)
                    return
                chunks = self.chunk_mapper.get_chunks(fh)
                offset = struct.unpack("!Q", data[2+fh_len:2+fh_len+8])[0]
                data_len = struct.unpack("!I", data[2+fh_len+8:2+fh_len+8+4])[0]
                # input -> chunk nums, rel_offset, read_len -> send msg
                data_read = b''
                while data_len > 0:
                    chunk_id = int(offset/chunk_limit)+1
                    chunk_name = bytes(to_hex_string(metaname[:3] + chunk_id.to_bytes(1, 'big')), "utf-8")
                    rel_offset = offset%5
                    read_len = chunk_limit - rel_offset
                    request_data = encode_read_request(chunk_name, rel_offset, read_len)
                    response = self.msg_broker.send(chunks[chunk_id], request_data)
                    data_read += response[5:]
                    offset+=read_len
                    data_len -= read_len
                response = struct.pack("!BI", 0x02, len(data_read)) + data_read
            case RequestType.WRITE.value:
                if metaname == None:
                    print(f"{fh} does not exist")
                    conn.sendall(response)
                    return
                offset = struct.unpack("!Q", data[2+fh_len:2+fh_len+8])[0]
                data_len = data[2+fh_len+8]
                data = data[2+fh_len+9:2+fh_len+9+data_len]
                metadata, metaname = self.chunk_mapper.get_metadata(fh)
                chunks = self.chunk_mapper.get_chunks(fh)
                num_chunks = metadata['num_chunks']
                if offset > (num_chunks+1)*chunk_limit:
                    print(f"Byte offset {offset} is out of range.")
                    conn.sendall(response)
                    return
                while data_len > 0:
                    chunk_id = int(offset/chunk_limit)+1
                    if chunk_id not in chunks.keys():
                        worker_port = self.select_worker()
                        new_chunk_name = self.chunk_mapper.insert_chunk(fh, worker_port)
                        new_chunk_name = bytes(to_hex_string(new_chunk_name), "utf-8")
                        self.msg_broker.send(worker_port, encode_create_request(new_chunk_name))
                        metadata, metaname = self.chunk_mapper.get_metadata(fh)
                        chunks = self.chunk_mapper.get_chunks(fh)
                        num_chunks = metadata['num_chunks']
                    rel_offset = offset%5
                    write_len = chunk_limit-rel_offset
                    worker_port = chunks[chunk_id]
                    chunk_name = to_hex_string(metaname[:3] + chunk_id.to_bytes(1, 'big'))
                    chunk_name_bytes = bytes(chunk_name, 'utf-8')
                    self.msg_broker.send(worker_port, encode_write_request(chunk_name_bytes, rel_offset, data[:write_len]))
                    offset += write_len
                    data_len -= write_len
                    data = data[write_len:]
                response = struct.pack("!BI", 0x03, 0)
        conn.sendall(response)

if __name__ == "__main__":
    host = "127.0.0.1"
    port = 1234
    db_manager = DBManager()
    chunk_mapper = ChunkMapper(db_manager)
    worker_ports = [1235, 1236, 1237]
    # keeps a handle on all the worker ports and forwards msg back and forth
    msg_broker = MessageBroker(worker_ports)
    # receives messages from clients and handles them
    msg_recv = MessageReceiver(host, port, msg_broker, db_manager, chunk_mapper)
    msg_recv.start()