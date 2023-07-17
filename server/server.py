import mysql.connector

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='pass',
        database='fsmap'
    )
    print("Successfully connected to MySQL")
except mysql.connector.Error as e:
    raise Exception("Error connecting to MySQL: " + str(e))

cursor = conn.cursor()

# cursor.execute("INSERT INTO file_to_node (filename, node) VALUES (%s, %s)", ("test", "nodetest"))

query = "SELECT * FROM file_to_node"
cursor.execute(query)

for row in cursor.fetchall():
    print(row)

conn.commit()
cursor.close()
conn.close()