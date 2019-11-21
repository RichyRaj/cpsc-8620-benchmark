import duckdb
import mysql.connector

from time import time
import random

class BenchmarkResult():
    '''
    Represents the result of a benchmark object
    '''
    def __init__(self):
        self.db_name = ""
        self.create = 0        
        self.s_insert = 0        
        self.m_insert = 0        
        self.s_select = 0        
        self.c_select = 0
        self.m_update = 0         

    def __str__(self):
        result_str = """
        =================== TigerDB Benchmark ========================
        ===================== Database : %s =====================
        Create Table : %s s
        Single Insert : %s s
        Multiple Inserts (Across Tables) : %s s
        Single Select : %s s
        Complex Select : %s s
        Multiple Update : %s s
        ======================== GO TIGERS ==========================
        =================== TigerDB Benchmark ========================
        """ % (str(self.db_name), str(self.create), str(self.s_insert), str(self.m_insert), str(self.s_select), str(self.c_select), str(self.m_update))
        return result_str


def test_duck_db(no_inserts=10000):
    '''
    Runs benchmark on Duck DB
    '''
    result = BenchmarkResult()
    result.db_name = "Duck DB"
    # Create an in-memory database
    con = duckdb.connect(':memory:')

    # To perform SQL commands
    c = con.cursor()

    # Create Statements
    s = time()
    c.execute("CREATE TABLE sensors(id INTEGER PRIMARY KEY NOT NULL, type VARCHAR(20), location VARCHAR(30))")
    c.execute("CREATE TABLE sensor_data(s_id INTEGER NOT NULL, temp DOUBLE, cpu DOUBLE)")
    st = time()
    result.create = st - s

    # FIll Table 1 with defaults
    c.execute("INSERT INTO sensors VALUES (1, 'a', 'floor')")
    c.execute("INSERT INTO sensors VALUES (2, 'b', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (3, 'a', 'floor')")
    c.execute("INSERT INTO sensors VALUES (4, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (5, 'b', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (6, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (7, 'b', 'floor')")
    c.execute("INSERT INTO sensors VALUES (8, 'a', 'floor')")
    c.execute("INSERT INTO sensors VALUES (9, 'b', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (10, 'a', 'floor')")
    c.execute("INSERT INTO sensors VALUES (11, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (12, 'b', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (13, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (14, 'b', 'floor')")
    c.execute("INSERT INTO sensors VALUES (15, 'a', 'floor')")

    # Auto inserts
    query_prefix = "INSERT INTO sensor_data VALUES ("
    s = time()
    for i in range(no_inserts):
        ith_id = random.randint(1, 15)
        ith_v1 = round(random.uniform(0, 100), 2)
        ith_v2 = round(random.uniform(0, 1), 5)
        query = query_prefix + str(ith_id) + ", " + str(ith_v1) + ", " + str(ith_v2) + ");"
        c.execute(query)
    st = time()
    result.m_insert = st - s
        # Simple Select
    s = time()
    c.execute("SELECT * FROM sensors")
    c.execute("SELECT COUNT(*) FROM sensor_data")
    print(c.fetchall())
    st = time()
    result.s_select = st - s
    return result

def test_mysql_db(no_inserts=10000):
    '''
    Runs the benchmark on DUCK DB
    '''
    result = BenchmarkResult()
    result.db_name = "MySQL"

    # Create the SQL
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="password" # Not the correct password. Obviously!.
    )
    
    # To perform SQL commands
    c = mydb.cursor(buffered=True)

    c.execute("USE cpsc8620;")
    c.execute("DROP TABLE IF EXISTS sensor_data;")
    c.execute("DROP TABLE IF EXISTS sensors;")
    # Create Statements
    s = time()
    c.execute("CREATE TABLE sensors(id INTEGER NOT NULL, type VARCHAR(20), location VARCHAR(30), PRIMARY KEY(id))")
    c.execute("CREATE TABLE sensor_data(s_id INTEGER NOT NULL, temp DOUBLE, cpu DOUBLE, FOREIGN KEY(s_id) REFERENCES sensors(id))")
    st = time()
    result.create = st - s

    # Table 1 Fill
    c.execute("INSERT INTO sensors VALUES (1, 'a', 'floor')")
    c.execute("INSERT INTO sensors VALUES (2, 'b', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (3, 'a', 'floor')")
    c.execute("INSERT INTO sensors VALUES (4, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (5, 'b', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (6, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (7, 'b', 'floor')")
    c.execute("INSERT INTO sensors VALUES (8, 'a', 'floor')")
    c.execute("INSERT INTO sensors VALUES (9, 'b', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (10, 'a', 'floor')")
    c.execute("INSERT INTO sensors VALUES (11, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (12, 'b', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (13, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (14, 'b', 'floor')")
    c.execute("INSERT INTO sensors VALUES (15, 'a', 'floor')")

    # Auto inserts
    query_prefix = "INSERT INTO sensor_data VALUES ("
    s = time()
    for i in range(no_inserts):
        ith_id = random.randint(1, 15)
        ith_v1 = round(random.uniform(0, 100), 2)
        ith_v2 = round(random.uniform(0, 1), 5)
        query = query_prefix + str(ith_id) + ", " + str(ith_v1) + ", " + str(ith_v2) + ");"
        c.execute(query)
    st = time()
    result.m_insert = st - s    
        # Simple Select
    s = time()
    c.execute("SELECT * FROM sensors")
    c.execute("SELECT COUNT(*) FROM sensor_data")
    print(c.fetchall())
    st = time()
    result.s_select = st - s
    return result
    

if __name__ == "__main__":
    r = test_duck_db()
    print(r)    
    r = test_mysql_db()
    print(r)
