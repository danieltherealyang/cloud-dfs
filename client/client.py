import socket
import struct

def create_request(filename):
    operation_type = b'\x01'
    filename_length = struct.pack('!B', len(filename))
    filename_bytes = filename.encode('ascii')

    request = operation_type + filename_length + filename_bytes
    return request

def read_request(file_handle, offset, num_bytes):
    operation_type = b'\x02'
    file_handle_length = struct.pack('!B', len(file_handle))
    file_handle_bytes = file_handle.encode('ascii')
    offset_bytes = struct.pack('!Q', offset)
    num_bytes_bytes = struct.pack('!I', num_bytes)

    request = operation_type + file_handle_length + file_handle_bytes + offset_bytes + num_bytes_bytes
    return request

def write_request(file_handle, offset, data):
    operation_type = b'\x03'
    file_handle_length = struct.pack('!B', len(file_handle))
    file_handle_bytes = file_handle.encode('ascii')
    offset_bytes = struct.pack('!Q', offset)
    data_length = struct.pack('!B', len(data))
    data_bytes = data.encode('ascii')

    request = operation_type + file_handle_length + file_handle_bytes + offset_bytes + data_length + data_bytes
    return request

def remove_request(filename):
    operation_type = b'\x04'
    filename_length = struct.pack('!B', len(filename))
    filename_bytes = filename.encode('ascii')

    request = operation_type + filename_length + filename_bytes
    return request

def send_request(request):
    host = '127.0.0.1'  # Server IP address
    port = 5432 # Server port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        client_socket.sendall(request)

        # Receive the response from the server
        response = client_socket.recv(4096)
        client_socket.close()

    return response

# Example usage:
if __name__ == "__main__":
    # socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(("127.0.0.1", 5432))
    create_request_data = create_request('example.txt')
    response = send_request(create_request_data)
    print(response)
