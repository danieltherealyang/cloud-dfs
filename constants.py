import enum

class RequestType(enum.Enum):
    CREATE = 0x01
    READ = 0x02
    WRITE = 0x03
    REMOVE = 0X04