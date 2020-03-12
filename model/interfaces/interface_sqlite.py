import os
import sqlite3
from sqlite3 import Error
import psutil

class sqliteInterface:

    def __init__(self):
        # -- database for simulation
        if os.path.exists(r'./database/MAS.db'):
            os.remove(r'./database/MAS.db')
        self.db_file = r'./database/MAS.db'

    def createTables(self):
        # -- Statment Agents
        sql_create_agents_table = """CREATE TABLE IF NOT EXISTS agents (id integer PRIMARY KEY,
                                                                name text NOT NULL,
                                                                typ text NOT NULL,
                                                                area text NOT NULL,
                                                                reserve text NOT NULL
                                                                ); """
        # -- Statment Orders
        sql_create_orders_table = """CREATE TABLE IF NOT EXISTS orders (
                                                    id integer PRIMARY KEY,
                                                    name text NOT NULL,
                                                    hour integer NOT NULL,
                                                    volume real NOT NULL,
                                                    price real NOT NULL
                                                );"""
        # -- Statment Processes
        sql_create_process_table = """CREATE TABLE IF NOT EXISTS process (
                                                            id integer PRIMARY KEY,
                                                            name text NOT NULL,
                                                            pid integer NOT NULL
                                                        );"""
        # -- Statment Actuals
        sql_create_actuals_table = """CREATE TABLE IF NOT EXISTS actuals (
                                                            id integer PRIMARY KEY,
                                                            name text NOT NULL,
                                                            hour integer NOT NULL,
                                                            volume real NOT NULL
                                                        );"""
        # -- Statment Actuals
        sql_create_balancing_table = """CREATE TABLE IF NOT EXISTS balancing (
                                                            id integer PRIMARY KEY,
                                                            name text NOT NULL,
                                                            slot integer NOT NULL,
                                                            volume real NOT NULL,
                                                            typ text NOT NULL,
                                                            powerPrice real NOT NULL,
                                                            energyPrice real NOT NULL
                                                        );"""

        try:
            conn = sqlite3.connect(self.db_file)                                                                    # -- create Database
            c = conn.cursor()
            c.execute(sql_create_agents_table)                                                                      # -- create tables
            c.execute(sql_create_orders_table)
            c.execute(sql_create_balancing_table)
            c.execute(sql_create_actuals_table)
            c.execute(sql_create_process_table)
            conn.commit()                                                                                           # -- close connection
            conn.close()

        except Error as e:
            print(e)

# -- DayAhead
    def setDayAhead(self, request):
        try:
            content = request.json
            name = content['uuid']
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name FROM agents WHERE name=?", (name,))
            if len(c.fetchall()) == 0:
                print('Agent %s does not logged in' % name)
            else:
                sql = ''' INSERT INTO orders(name,hour,volume,price)
                            VALUES (?,?,?,?) '''
                for i in range(24):
                    for k in range(len(content[str(i)])):
                        order = (name, i, content[str(i)][k][0], content[str(i)][k][1])
                        c.execute(sql, order)
                conn.commit()
                conn.close()
        except Error as e:
            print(e)

    def getDayAhead(self,name):
        orders = []
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name,hour,price,volume FROM orders WHERE name=?", (name,))
            orders = c.fetchall()
            conn.close()
        except Error as e:
            print(e)
        return orders

    def deleteDayAhead(self):
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("DELETE FROM orders")
            conn.commit()
            conn.close()
        except Error as e:
            print(e)

# -- Balancing
    def setBalancing(self, request):
        try:
            name = request['uuid']
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name FROM agents WHERE name=?", (name,))
            if len(c.fetchall()) == 0:
                print('Agent %s does not logged in' % name)
            else:
                sql = ''' INSERT INTO balancing(name,slot,volume,typ,powerPrice,energyPrice)
                            VALUES (?,?,?,?,?,?) '''
                for i in range(6):
                    for k in ['pos','neg']:
                        order = (name, i, request[str(i) + '_' + k][0], k,
                                 request[str(i) + '_' + k][1], request[str(i) + '_' + k][2])
                        c.execute(sql, order)
                conn.commit()
                conn.close()
        except Error as e:
            print(e)

    def getBalancing(self, name):
        balancing = []
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name,slot,volume,typ,powerPrice,energyPrice FROM balancing WHERE name=?", (name,))
            balancing = c.fetchall()
            conn.close()
        except Error as e:
            print(e)
        return balancing

    def deleteBalancing(self):
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("DELETE FROM balancing")
            conn.commit()
            conn.close()
        except Error as e:
            print(e)

        # ----- Set Actuals for each Agent -----
        def setActuals(self, request):
            try:
                content = request.json
                name = content['uuid']
                conn = sqlite3.connect(self.db_file)
                c = conn.cursor()
                c.execute("SELECT name FROM agents WHERE name=?", (name,))
                if len(c.fetchall()) == 0:
                    print('Agent %s does not logged in' % name)
                else:
                    sql = ''' INSERT INTO actuals(name,hour,volume)
                                                VALUES (?,?,?) '''
                    for i in range(24):
                        actual = (name, i, content[str(i)])
                        c.execute(sql, actual)
                    conn.commit()
                    conn.close()
            except Error as e:
                print(e)

        # ----- get actuals of any Agent -----
        def getActual(self, name):
            actuals = []
            try:
                conn = sqlite3.connect(self.db_file)
                c = conn.cursor()
                c.execute("SELECT name,hour,volume FROM actuals WHERE name=?", (name,))
                actuals = c.fetchall()
                conn.close()
            except Error as e:
                print(e)
            return actuals

        # ----- delete all actuals -----
        def deleteActuals(self):
            try:
                conn = sqlite3.connect(self.db_file)
                c = conn.cursor()
                c.execute("DELETE FROM actuals")
                conn.commit()
                conn.close()
            except Error as e:
                print(e)

    def getBalancingAgents(self):
        agents = []
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name FROM agents WHERE reserve=?", ('X',))
            rows = c.fetchall()
            agents = [row[0] for row in rows]
            conn.close()
        except Error as e:
            print(e)
        return agents

# -- Actuals
    def setActuals(self, request):
        try:
            content = request.json
            name = content['uuid']
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name FROM agents WHERE name=?", (name,))
            if len(c.fetchall()) == 0:
                print('Agent %s does not logged in' % name)
            else:
                sql = ''' INSERT INTO actuals(name,hour,volume)
                                            VALUES (?,?,?) '''
                for i in range(24):
                    actual = (name, i, content[str(i)])
                    c.execute(sql, actual)
                conn.commit()
                conn.close()
        except Error as e:
            print(e)

    def getActual(self, name):
        actuals = []
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name,hour,volume FROM actuals WHERE name=?", (name,))
            actuals = c.fetchall()
            conn.close()
        except Error as e:
            print(e)
        return actuals

    def deleteActuals(self):
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("DELETE FROM actuals")
            conn.commit()
            conn.close()
        except Error as e:
            print(e)

# -- Services
    def startServices(self, name, pid):
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            sql = ''' INSERT INTO process(name,pid) VALUES (?,?) '''
            c.execute(sql,(name, pid))
            conn.commit()
            conn.close()
        except Error as e:
            print(e)

    def stopServices(self):
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT pid FROM process")
            rows = c.fetchall()
            pids = [row[0] for row in rows]
            for pid in pids:
                try:
                    process = psutil.Process(pid)
                    for proc in process.children(recursive=True):
                        proc.kill()
                    process.kill()
                except Exception as e:
                    print(e)
            c.execute("DELETE FROM process")
            conn.commit()
            conn.close()
        except Error as e:
            print(e)

# -- Agents
    def getNumberAgentTyps(self,typ):
        dict_ = {}
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name FROM agents WHERE typ=?", (typ,))
            a = [row[0] for row in c.fetchall()]
            dict_ = {'Agent %i' % i: a[i] for i in range(0, len(a))}
            conn.close()
        except Error as e:
            print(e)
        return dict_

    def getAllAgents(self):
        agents = []
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name FROM agents")
            rows = c.fetchall()
            agents = [row[0] for row in rows]
            conn.close()
        except Error as e:
            print(e)
        return agents

    def loginAgent(self, request):
        try:
            content = request.json
            # content = request
            name = content['uuid']
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name FROM agents WHERE name=?", (name,))
            if len(c.fetchall()) == 0:
                sql = ''' INSERT INTO agents(name, typ, area, reserve) VALUES (?,?,?,?) '''
                c.execute(sql,(content['uuid'], content['typ'],content['area'],content['reserve']))
            else:
                print('agent already logged in')
            conn.commit()
            conn.close()
        except Error as e:
            print(e)

    def logoutAgent(self, request):
        try:
            content = request.json
            name = content['uuid']
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT name FROM agents WHERE name=?", (name,))
            if len(c.fetchall()) == 0:
                print('agent not logged in')
            else:
                c.execute("DELETE FROM agents WHERE name=?", (name,))
            conn.commit()
            conn.close()
        except Error as e:
            print(e)