import sqlalchemy
import sqlite3
import json
import os


def set_default():
    print("To create a default insert the name of an existing credential")
    default = input("-> ")
    config = {'default':default}

    fname = os.path.expanduser('~').replace('\\','/') + '/default.json'
    with open(fname, 'w') as f:
        json.dump(config, f)
    print("Saved default file: ", fname)


def create_credentials():
    print("Welcome to the credentail creator. This script helps create different json credentials for server connectivity. While rudementary, it works! For ease of use, the default save location is the user's root directory.")
    run = True

    print("Beginning creation of new credential ... ")
    server_type = input("database type (currently accepted: mysql, postgres, mssql+pyodbc): ")
    user = input("username: ")
    pwd = input("password: ")
    host = input("ip addr: ")
    port = input("port: ")
    database = input("database: ")
    
    config = {"s_type":server_type, "user":user, "password":pwd, "host":host, "port":port, "database":database}
    
    fname = input("filename: ") + '.json'
    fname = os.path.expanduser('~').replace('\\','/') + '/'  + fname
    with open(fname, 'w') as f:
        json.dump(config, f)
    print("File saved to ", fname)
    print()



class Connector():
    """
    Serves as point of reference for connection to internal database, currently supports mysql and postgres
    
    ...
    Parameters
    ----------
    cred_name : string, optional
        name of credentials to connect from, if None uses 'default' (see: set_default())
    """
    def __init__(self, cred_name = None):
        if cred_name is None:
            cred_name = 'default'
        with open(os.path.expanduser('~').replace('\\','/') + '/' + cred_name + '.json', 'r') as f:
            self.credentials = json.load(f)
        self.user = self.credentials['user']
        self.password = self.credentials['password']
        self.host = self.credentials['host']
        self.port = self.credentials['port']
        self.db = self.credentials['database']
        self.s_type = self.credentials['s_type']

    def get_credentials(self):
        return self.credentials

    def cnx(self):
        """
        returns connection relative to type of server type, either a mysql.connector or psycopg2
        """
        if self.s_type == 'sqlite':
            return sqlite3.connect(self.host)
        #return mysql.connect(**{x:v for x, v in self.credentials.items() if x != 's_type'})

    def engine(self):
        """
        return sqlalchemy engine object using the connection parameters 
        """
        return sqlalchemy.create_engine(f'sqlite:///{self.host}')
