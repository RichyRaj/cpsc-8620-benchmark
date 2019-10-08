import duckdb
from time import time
import mysql.connector

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

    def __str__(self):
        result_str = """
        =================== TigerDB Benchmark ========================
        ===================== Database : %s =====================
        Create Table : %s s
        Single Insert : %s s
        Multiple Inserts (Across Tables) : %s s
        Single Select : %s s
        Complex Select : %s s
        ======================== GO TIGERS ==========================
        =================== TigerDB Benchmark ========================
        """ % (str(self.db_name), str(self.create), str(self.s_insert), str(self.m_insert), str(self.s_select), str(self.c_select))
        return result_str

def test_duck_db():
    '''
    Runs the benchmark on DUCK DB
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

    # Single Insert
    s = time()
    c.execute("INSERT INTO sensors VALUES (1, 'a', 'floor')")
    st = time()
    result.s_insert = st - s

    # Multiple Inserts - Across Tables
    s = time()
    c.execute("INSERT INTO sensors VALUES (2, 'b', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (3, 'a', 'floor')")
    c.execute("INSERT INTO sensors VALUES (4, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (5, 'b', 'ceiling')")
    # Second Table
    c.execute("INSERT INTO sensor_data VALUES (1, 92.23, 0.87222)")
    c.execute("INSERT INTO sensor_data VALUES (2, 52.23, 0.37222)")
    c.execute("INSERT INTO sensor_data VALUES (3, 22.23, 0.57222)")
    c.execute("INSERT INTO sensor_data VALUES (4, 12.23, 0.27222)")
    c.execute("INSERT INTO sensor_data VALUES (5, 32.23, 0.17222)")
    c.execute("INSERT INTO sensor_data VALUES (1, 12.23, 0.17222)")
    c.execute("INSERT INTO sensor_data VALUES (2, 22.23, 0.57222)")
    c.execute("INSERT INTO sensor_data VALUES (3, 12.23, 0.27222)")
    c.execute("INSERT INTO sensor_data VALUES (4, 12.23, 0.57222)")
    c.execute("INSERT INTO sensor_data VALUES (5, 52.23, 0.67222)")
    st = time()
    result.m_insert = st - s

    # Simple Select
    s = time()
    c.execute("SELECT * FROM sensors")
    c.execute("SELECT * FROM sensor_data")
    st = time()
    result.s_select = st - s

    # JOIN + AGGREGATE + SELECT
    complex_query = """
    SELECT 
    sensors.location,
    MAX(sensor_data.temp) as total_temp,
    MAX(sensor_data.cpu) as total_cpu
    From 
    sensors
    LEFT JOIN sensor_data
        ON sensors.id = sensor_data.s_id
    GROUP BY location;
    """
    s = time()
    c.execute(complex_query)
    st = time()
    result.c_select = st - s

    print(c.fetchall())
    return result

def test_mysql_db():
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

    # Single Insert
    s = time()
    c.execute("INSERT INTO sensors VALUES (1, 'a', 'floor')")
    st = time()
    result.s_insert = st - s

    # Multiple Inserts - Across Tables
    s = time()
    c.execute("INSERT INTO sensors VALUES (2, 'b', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (3, 'a', 'floor')")
    c.execute("INSERT INTO sensors VALUES (4, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (5, 'b', 'ceiling')")
    # Second Table
    c.execute("INSERT INTO sensor_data VALUES (1, 92.23, 0.87222)")
    c.execute("INSERT INTO sensor_data VALUES (2, 52.23, 0.37222)")
    c.execute("INSERT INTO sensor_data VALUES (3, 22.23, 0.57222)")
    c.execute("INSERT INTO sensor_data VALUES (4, 12.23, 0.27222)")
    c.execute("INSERT INTO sensor_data VALUES (5, 32.23, 0.17222)")
    c.execute("INSERT INTO sensor_data VALUES (1, 12.23, 0.17222)")
    c.execute("INSERT INTO sensor_data VALUES (2, 22.23, 0.57222)")
    c.execute("INSERT INTO sensor_data VALUES (3, 12.23, 0.27222)")
    c.execute("INSERT INTO sensor_data VALUES (4, 12.23, 0.57222)")
    c.execute("INSERT INTO sensor_data VALUES (5, 52.23, 0.67222)")
    st = time()
    result.m_insert = st - s

    # Simple Select
    s = time()
    c.execute("SELECT * FROM sensors")
    c.execute("SELECT * FROM sensor_data")
    st = time()
    result.s_select = st - s

    # JOIN + AGGREGATE + SELECT
    complex_query = """
    SELECT 
    sensors.location,
    MAX(sensor_data.temp) as total_temp,
    MAX(sensor_data.cpu) as total_cpu
    From 
    sensors
    LEFT JOIN sensor_data
        ON sensors.id = sensor_data.s_id
    GROUP BY location;
    """
    s = time()
    c.execute(complex_query)
    st = time()
    result.c_select = st - s

    print(c.fetchall())
    return result

if __name__ == "__main__":
    r = test_duck_db()
    print(r)    
    r = test_mysql_db()
    print(r)
