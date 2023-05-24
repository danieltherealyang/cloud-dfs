import os
import socket
import struct

class DFSServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

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
        print(f"Received data: {data}")

        if data[0] == 0x01:  # CREATE request
            # Extract filename from request
            filename_len = data[1]
            filename = data[2:2+filename_len].decode()

            # Create file and send file handle back to client
            with open(filename, 'w') as f:
                pass
            fh = os.path.abspath(filename)
            response = struct.pack("!IB", 0x01, len(fh)) + fh.encode()
            conn.sendall(response)

        elif data[0] == 0x02:  # READ request
            # Extract file handle, offset, and count from request
            fh_len = data[1]
            fh = data[2:2+fh_len].decode()
            offset = struct.unpack("!Q", data[2+fh_len:2+fh_len+8])[0]
            count = struct.unpack("!I", data[2+fh_len+8:2+fh_len+8+4])[0]

            # Read specified portion of file and send data back to client
            with open(fh, 'r') as f:
                f.seek(offset)
                data = f.read(count)
            response = struct.pack("!IB", 0x02, len(data)) + data.encode()
            conn.sendall(response)

        elif data[0] == 0x03:  # WRITE request
            # Extract file handle, offset, and data from request
            fh_len = data[1]
            fh = data[2:2+fh_len].decode()
            offset = struct.unpack("!Q", data[2+fh_len:2+fh_len+8])[0]
            data_len = data[2+fh_len+8]
            data = data[2+fh_len+9:2+fh_len+9+data_len].decode()

            # Write data to file and send status code back to client
            with open(fh, 'r+') as f:
                f.seek(offset)
                f.write(data)
            response = struct.pack("!IB", 0x03, 0)
            conn.sendall(response)

        elif data[0] == 0x04:  # REMOVE request
            # Extract filename from request
            filename_len = data[1]
            filename = data[2:2+filename_len].decode()

            # Remove file and send status code back to client
            os.remove(filename)
            response = struct.pack("!IB", 0x04, 0)
            conn.sendall(response)

        conn.close()

if __name__ == "__main__":
    host = '0.0.0.0' # listen on all available interfaces
    port = 1234
    server = DFSServer(host, port)
    server.start()