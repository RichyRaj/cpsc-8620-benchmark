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
    c.execute("CREATE TABLE sensor_data(ts TIMESTAMP, s_id INTEGER NOT NULL, temp DOUBLE, cpu DOUBLE)")
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
    c.execute("INSERT INTO sensors VALUES (6, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (7, 'b', 'floor')")
    c.execute("INSERT INTO sensors VALUES (8, 'a', 'floor')")
    # Second Table
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 92.23, 0.84422)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 52.23, 0.22422)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 22.23, 0.65722)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 12.23, 0.27256)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 32.23, 0.17234)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 12.23, 0.13445)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 22.23, 0.54355)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 12.23, 0.65543)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 12.23, 0.53455)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 52.23, 0.63345)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 95.13, 0.45433)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 56.53, 0.64343)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 62.43, 0.35435)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 22.43, 0.26443)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 32.53, 0.17545)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 62.73, 0.53434)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 22.63, 0.57645)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 72.53, 0.56666)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 22.33, 0.78865)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 42.23, 0.45456)"), 
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 55.33, 0.43332)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 76.53, 0.35532)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 66.43, 0.57222)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 87.33, 0.67672)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 12.23, 0.17565)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 15.13, 0.22344)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 45.93, 0.57225)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 65.73, 0.27222)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 12.63, 0.29778)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 22.63, 0.89867)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 77.22, 0.72442)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 55.23, 0.64323)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 43.25, 0.64322)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 65.45, 0.86544)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 23.43, 0.45643)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 34.34, 0.76544)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 35.23, 0.53543)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 45.33, 0.53454)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 55.45, 0.99849)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 32.55, 0.04578)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 12.64, 0.21244)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 22.33, 0.34534)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 32.64, 0.55433)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 42.64, 0.98358)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 52.63, 0.10483)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 62.34, 0.48984)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 72.13, 0.33234)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 82.76, 0.29849)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 92.43, 0.54233)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 82.44, 0.98433)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 92.23, 0.82322)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 52.23, 0.52332)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 22.23, 0.98593)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 12.23, 0.34532)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 32.23, 0.14324)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 12.23, 0.10490)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 22.23, 0.54389)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 12.23, 0.22244)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 12.23, 0.24343)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 52.23, 0.45342)")   
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 88.45, 0.83242)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 76.54, 0.32344)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 35.24, 0.57222)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 65.65, 0.27343)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 34.23, 0.17423)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 34.54, 0.17344)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 26.53, 0.54524)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 45.52, 0.27432)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 88.56, 0.57253)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 58.75, 0.67345)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 23.43, 0.87587)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 54.86, 0.58754)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 66.66, 0.58989)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 44.88, 0.09958)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 78.53, 0.22849)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 86.23, 0.94869)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 33.54, 0.25455)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 76.22, 0.64343)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 33.11, 0.56443)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 45.33, 0.63643)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 44.26, 0.46986)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 67.45, 0.89943)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 13.64, 0.40983)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 65.22, 0.48436)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 22.34, 0.10968)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 45.77, 0.15658)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 64.56, 0.24555)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 64.77, 0.50690)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 88.56, 0.56095)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 43.66, 0.09202)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 22.27, 0.98739)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 52.46, 0.38309)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 99.45, 0.63093)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 56.34, 0.29593)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 33.76, 0.16345)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 75.66, 0.54565)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 43.44, 0.75656)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 76.54, 0.39866)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 33.23, 0.12345)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 75.43, 0.66565)")
    st = time()
    result.m_insert = st - s

    # Update Query
    s = time()
    c.execute("UPDATE sensor_data SET cpu = 0.66565 WHERE s_id = 8 AND temp = 75.43")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    c.execute("UPDATE sensor_data SET cpu = 0.09202 WHERE s_id = 5 AND temp = 43.66")
    c.execute("UPDATE sensor_data SET cpu = 0.56095 WHERE s_id = 4 AND temp = 43.66") 
    c.execute("UPDATE sensor_data SET cpu = 0.66565 WHERE s_id = 8 AND temp = 75.43")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    c.execute("UPDATE sensor_data SET cpu = 0.09202 WHERE s_id = 5 AND temp = 43.66")
    c.execute("UPDATE sensor_data SET cpu = 0.56095 WHERE s_id = 4 AND temp = 43.66") 
    c.execute("UPDATE sensor_data SET cpu = 0.50690 WHERE s_id = 6 AND temp = 64.77")    
    c.execute("UPDATE sensor_data SET cpu = 0.66565 WHERE s_id = 8 AND temp = 75.43")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    c.execute("UPDATE sensor_data SET cpu = 0.09202 WHERE s_id = 5 AND temp = 43.66")
    c.execute("UPDATE sensor_data SET cpu = 0.56095 WHERE s_id = 4 AND temp = 43.66") 
    c.execute("UPDATE sensor_data SET cpu = 0.50690 WHERE s_id = 6 AND temp = 64.77")
    c.execute("UPDATE sensor_data SET cpu = 0.66565 WHERE s_id = 8 AND temp = 75.43")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    c.execute("UPDATE sensor_data SET cpu = 0.09202 WHERE s_id = 5 AND temp = 43.66")
    c.execute("UPDATE sensor_data SET cpu = 0.56095 WHERE s_id = 4 AND temp = 43.66") 
    c.execute("UPDATE sensor_data SET cpu = 0.50690 WHERE s_id = 6 AND temp = 64.77")
    c.execute("UPDATE sensor_data SET cpu = 0.66565 WHERE s_id = 8 AND temp = 75.43")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    c.execute("UPDATE sensor_data SET cpu = 0.09202 WHERE s_id = 5 AND temp = 43.66")
    c.execute("UPDATE sensor_data SET cpu = 0.56095 WHERE s_id = 4 AND temp = 43.66") 
    c.execute("UPDATE sensor_data SET cpu = 0.50690 WHERE s_id = 6 AND temp = 64.77")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    st = time()
    result.m_update = st - s

    # Simple Select
    s = time()
    c.execute("SELECT * FROM sensors")
    c.execute("SELECT COUNT(*) FROM sensor_data")
    # c.execute("SELECT * FROM sensor_data")
    print(c.fetchall())
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
    c.execute("CREATE TABLE sensor_data(ts TIMESTAMP, s_id INTEGER NOT NULL, temp DOUBLE, cpu DOUBLE, FOREIGN KEY(s_id) REFERENCES sensors(id))")
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
    c.execute("INSERT INTO sensors VALUES (6, 'a', 'ceiling')")
    c.execute("INSERT INTO sensors VALUES (7, 'b', 'floor')")
    c.execute("INSERT INTO sensors VALUES (8, 'a', 'floor')")
    # Second Table
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 92.23, 0.84422)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 52.23, 0.22422)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 22.23, 0.65722)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 12.23, 0.27256)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 32.23, 0.17234)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 12.23, 0.13445)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 22.23, 0.54355)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 12.23, 0.65543)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 12.23, 0.53455)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 52.23, 0.63345)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 95.13, 0.45433)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 56.53, 0.64343)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 62.43, 0.35435)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 22.43, 0.26443)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 32.53, 0.17545)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 62.73, 0.53434)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 22.63, 0.57645)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 72.53, 0.56666)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 22.33, 0.78865)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 42.23, 0.45456)"), 
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 55.33, 0.43332)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 76.53, 0.35532)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 66.43, 0.57222)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 87.33, 0.67672)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 12.23, 0.17565)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 15.13, 0.22344)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 45.93, 0.57225)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 65.73, 0.27222)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 12.63, 0.29778)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 22.63, 0.89867)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 77.22, 0.72442)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 55.23, 0.64323)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 43.25, 0.64322)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 65.45, 0.86544)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 23.43, 0.45643)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 34.34, 0.76544)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 35.23, 0.53543)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 45.33, 0.53454)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 55.45, 0.99849)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 32.55, 0.04578)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 12.64, 0.21244)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 22.33, 0.34534)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 32.64, 0.55433)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 42.64, 0.98358)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 52.63, 0.10483)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 62.34, 0.48984)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 72.13, 0.33234)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 82.76, 0.29849)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 92.43, 0.54233)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 82.44, 0.98433)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 92.23, 0.82322)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 52.23, 0.52332)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 22.23, 0.98593)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 12.23, 0.34532)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 32.23, 0.14324)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 12.23, 0.10490)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 22.23, 0.54389)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 12.23, 0.22244)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 12.23, 0.24343)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 52.23, 0.45342)")   
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 88.45, 0.83242)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 76.54, 0.32344)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 35.24, 0.57222)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 65.65, 0.27343)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 34.23, 0.17423)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 34.54, 0.17344)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 26.53, 0.54524)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 45.52, 0.27432)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 88.56, 0.57253)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 58.75, 0.67345)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 23.43, 0.87587)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 54.86, 0.58754)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 66.66, 0.58989)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 44.88, 0.09958)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 78.53, 0.22849)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 86.23, 0.94869)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 33.54, 0.25455)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 76.22, 0.64343)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 33.11, 0.56443)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 45.33, 0.63643)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 44.26, 0.46986)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 67.45, 0.89943)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 13.64, 0.40983)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 65.22, 0.48436)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 22.34, 0.10968)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 45.77, 0.15658)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 64.56, 0.24555)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 64.77, 0.50690)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 88.56, 0.56095)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 43.66, 0.09202)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 22.27, 0.98739)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 52.46, 0.38309)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 7, 99.45, 0.63093)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 5, 56.34, 0.29593)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 6, 33.76, 0.16345)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 1, 75.66, 0.54565)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 2, 43.44, 0.75656)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 3, 76.54, 0.39866)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 4, 33.23, 0.12345)")
    c.execute("INSERT INTO sensor_data VALUES ('2008-01-01 00:00:01', 8, 75.43, 0.66565)")
    st = time()
    result.m_insert = st - s

    # Update Query
    s = time()
    c.execute("UPDATE sensor_data SET cpu = 0.66565 WHERE s_id = 8 AND temp = 75.43")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    c.execute("UPDATE sensor_data SET cpu = 0.09202 WHERE s_id = 5 AND temp = 43.66")
    c.execute("UPDATE sensor_data SET cpu = 0.56095 WHERE s_id = 4 AND temp = 43.66") 
    c.execute("UPDATE sensor_data SET cpu = 0.66565 WHERE s_id = 8 AND temp = 75.43")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    c.execute("UPDATE sensor_data SET cpu = 0.09202 WHERE s_id = 5 AND temp = 43.66")
    c.execute("UPDATE sensor_data SET cpu = 0.56095 WHERE s_id = 4 AND temp = 43.66") 
    c.execute("UPDATE sensor_data SET cpu = 0.50690 WHERE s_id = 6 AND temp = 64.77")    
    c.execute("UPDATE sensor_data SET cpu = 0.66565 WHERE s_id = 8 AND temp = 75.43")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    c.execute("UPDATE sensor_data SET cpu = 0.09202 WHERE s_id = 5 AND temp = 43.66")
    c.execute("UPDATE sensor_data SET cpu = 0.56095 WHERE s_id = 4 AND temp = 43.66") 
    c.execute("UPDATE sensor_data SET cpu = 0.50690 WHERE s_id = 6 AND temp = 64.77")
    c.execute("UPDATE sensor_data SET cpu = 0.66565 WHERE s_id = 8 AND temp = 75.43")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    c.execute("UPDATE sensor_data SET cpu = 0.09202 WHERE s_id = 5 AND temp = 43.66")
    c.execute("UPDATE sensor_data SET cpu = 0.56095 WHERE s_id = 4 AND temp = 43.66") 
    c.execute("UPDATE sensor_data SET cpu = 0.50690 WHERE s_id = 6 AND temp = 64.77")
    c.execute("UPDATE sensor_data SET cpu = 0.66565 WHERE s_id = 8 AND temp = 75.43")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    c.execute("UPDATE sensor_data SET cpu = 0.09202 WHERE s_id = 5 AND temp = 43.66")
    c.execute("UPDATE sensor_data SET cpu = 0.56095 WHERE s_id = 4 AND temp = 43.66") 
    c.execute("UPDATE sensor_data SET cpu = 0.50690 WHERE s_id = 6 AND temp = 64.77")
    c.execute("UPDATE sensor_data SET cpu = 0.12345 WHERE s_id = 4 AND temp = 33.23")
    st = time()
    result.m_update = st - s

    # Simple Select
    s = time()
    c.execute("SELECT * FROM sensors")
    c.execute("SELECT COUNT(*) FROM sensor_data")
    # c.execute("SELECT * FROM sensor_data")
    print(c.fetchall())
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
