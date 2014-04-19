import pyodbc
import os
import pandas as pd
import pandas.io.sql as psql
from datetime import datetime, date
import numpy as np
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, create_engine
from sqlalchemy.sql import select
from sqlalchemy.types import Integer, Float, BigInteger, Boolean, String, Date, DateTime, Text
import datetime

class Sql:

    def __init__(self, dsn, db = 'mssql'):
        """
        a class to support reading and writing data from SQL Server

        Parameters
        ----------
        dsn : string
            specifies which database to connect to, comes from user's odbc.ini file

        db : ['mssql', 'sqlite'], default = 'mssql'
            specifies the RDBMS you want to use. If you are unsure, then you don't
            need to use this. When using sqlite, remember that the dsn is the
            absolute location of your .db file (i.e.: /home/user/mydb.db).

        Methods
        -------
        See the functions below
        """
        if db == 'mssql':
            engine_url = 'mssql+pyodbc://%s' % dsn 
        else:
            engine_url = 'sqlite:///%s' % dsn

        engine = create_engine(engine_url)
        metadata = MetaData()
        metadata.reflect(bind = engine)

        self.dsn = dsn
        self.engine = engine
        self.metadata = metadata

    def query(self, query):
        """
        read data from SQL Server into pandas DataFrame

        Parameters
        ----------
        query : string 
            SQL statement you wish to execute

        Returns
        -------
        res : pandas.DataFrame
            results from executing `query`
        """
        cnxn = pyodbc.connect(Trusted_Connection='yes', dsn = self.dsn)
        res = psql.read_frame(query,cnxn)
        cnxn.close()
        return res

    def create_table(self, table_query):
        """
        create a table in SQL Server

        Parameters

        -----------
        table_query : string
            SQL query of form 'CREATE TABLE'

        Returns
        -------
        None
        """
        cnxn = pyodbc.connect(Trusted_Connection='yes', dsn = self.dsn)
        cur = cnxn.cursor()
        cur.execute(table_query)
        cnxn.commit()
        cnxn.close()

    def import_table(self, table_name, output = 'dict', index_name = None):
        """
        import a table from SQL Server into either a list of dictionaries or 
        a list of dictionaries

        Parameters
        ----------
        table_name : string
            name of the table you wish to access
        output : {'dict', 'df'}, default = dict
            specifies the output of the operation, whether a list of
            dictionaries or a pandas dataframe
        index_name : string
            name of the column where your index resides

        Returns
        -------
        None
        """
        conn = (self.engine).connect()
        table = (self.metadata).tables[table_name]
        s = select([table])
        result = conn.execute(s)
        keys = result._metadata.keys
        result_list = []
         
        for row in result:
            values = list(row)
            data = dict(zip(keys, values))
            result_list.append(data)

        if output == 'df':
            df = pd.DataFrame(result_list)
            
            if index_name:
                df.index = df[index_name]
                df = df.drop(index_name, axis = 1)
            
            return df
        
        else:
            return result_list

    def insert(self, table_name, data):
        """
        inserts data into a SQL table, can be used iteratively or as a batch

        Parameters
        ----------
        table_name : string
            name of the table you wish to acces
        data : list of dictionaries
            a list containing the dictionaries where your data resides,
            the keys in these dictionaries must line up with the column
            names in your table and must all be the same. Can also be a 
            Pandas DataFrame that will be converted.

        Returns
        -------
        None
        """
        if type(data) == 'pandas.core.frame.DataFrame':
            data = Sql.df2dict(self, data)

        table = (self.metadata).tables[table_name]
        ins = table.insert()
        conn = (self.engine).connect()
        conn.execute(ins, data)
        conn.close()
    
    def write_table(self, data, table_name, if_exists = 'append', create = False, index = False, char_limit = 255):
        """
        Allows the user to write data to SQL server directly with a completed
        data set. Will also create tables to fit the features of your data. if
        using iteratively it is advised you use Sql.insert.

        Parameters
        ----------
        data : pandas dataframe or a list of dictionaries
            The data you want to write to SQL, if passes as a dataframe the 
            operations will proceed as normal, if a list of dictionaries the
            data will be transformed to a pandas dataframe.
        table_name : string
            The name of the table you want to create/ write to.
        if_exists : ['append', 'fail', 'replace'], default = 'append'
            Tells the function how to handle the operation if the table
            already exists. Append will add to the end of the table, fail
            will throw an error and replace will delete the table and write
            a new one with the same name.
        create : bool, default = False
            True will create an empty table, false will insert the data
        index : bool, default = False
            True will use index as primary key, false will ignore it
        char_limit : int, default = 255
            The maximum number of characters allowed in varchar fields
        """
        if isinstance(data, list) or isinstance(data, dict):
            data = Sql.dict2df(self, data)

        if if_exists not in ['append', 'fail', 'replace']:
            raise ValueError("%s is not a valid if_exists value.") % table_name

        existence = (self.engine).dialect.has_table((self.engine).connect(), table_name)

        if existence:
            if if_exists == 'fail':
                raise ValueError("Table %s already exists.") % table_name
            elif if_exists == 'replace':
                Sql.drop_table(self, table_name)
                (self.metadata).remove((self.metadata).tables[table_name])
            elif if_exists == 'append':
                data = Sql.df2dict(self, data)
                Sql.insert(self, table_name, data)

        existence = (self.engine).dialect.has_table((self.engine).connect(), table_name)
        
        if existence == False:
            column_names = [col for col in data.columns]
            column_types = []

            for col in column_names:
                indexed = data.index[0]
                value = data[col][indexed]
                column_types.append(_sql_dtypes(value, char_limit))

            col_query = [Column(col, dtype) for col, dtype in zip(column_names, column_types)]

            if index:
                col_query.insert(0, Column('Index', _sql_dtypes(data.index[0]), index = True))
                
            table = Table(table_name, self.metadata, *col_query)
            table.create(self.engine)
            
            if not create:
                data = Sql.df2dict(self,data)
                Sql.insert(self, table_name, data)

    def drop_table(self, table_name):
        """
        Permanently drops a table from SQL Server
        
        Parameters
        --------
        table_name : string
            the name of the table you wish to drop
        
        Returns
        -------
        None
        """
        table = (self.metadata).tables[table_name]
        table.drop(self.engine)

    def df2dict(self, df):
        """
        Converts pandas dataframes into a list of dictionaries, so the data
        can be written with Sql.insert

        Parameters
        ----------
        df : pandas dataframe
            dataframe where your data is stored

        Returns
        -------
        data : list of dictionaries
        """
        data = df.to_dict('records')
        return data

    def dict2df(self, data):
        """
        Converts lists of dictionaries to pandas dataframe for convenient 
        table writing

        Parameters
        ----------
        data : list of dictionaries or dictionary
            data that is to be converted to dataframe

        Returns
        -------
        df : pandas dataframe
        """
        df = pd.DataFrame(data)
        return df

def create_odbc_ini():
    """Creates odbc.ini file which stores user specified dsn's"""
    here = os.getcwd()
    usr = os.getlogin()
    if os.name != 'posix':
        return "Error: Must be on the linux server to make an odbc.ini file"
    elif os.path.exists(os.path.abspath('.odbc.ini')):
        return "Error: odbc.ini already exists"
    else:
        try:
            os.chdir("/home/"+usr)
            with open('.odbc.ini', 'w') as f:
                f.write("[Data Sources]\n\n[Default]\nDriver = /usr/local/lib/libtdsodbc.so\n")
            os.chdir(here)
        except Exception as err:
            os.chdir(here)
            print str(err)

def add_dsn(dsn, server, db):
    """
    add new dsn to odbc.ini

    Parameters
    ----------
    dsn : string
        User specified name for connection
    server : string
        Server where database is kept, e.g., devsql10
    db : string 
        name of database, e.g., pdb_ALS_Renewal
    Returns
    -------
    None

    """
    if os.name != 'posix':
        return 'Error: Must be on linux server to add a connection with this function'
    else:
        here = os.getcwd()
        usr = os.getlogin()
        try:
            os.chdir("/home/"+usr)
            with open('.odbc.ini', 'r+') as f:
                txt = f.read()
                f.seek(0)
                f.write(txt[:txt.find("Default")-2]+"["+dsn+"]\nDriver = /usr/local/lib/libtdsodbc.so\nDescription = MS SQL Server\nTrace = No\nServername = "+server+"\nDatabase = "+db+"\n\n"+txt[txt.find("Default")-2:])
            os.chdir(here)
        except Exception as err:
            os.chdir(here)
            print str(err)

def get_dsn_list():
    """returns list of dsns"""
    if os.name != 'posix':
        return 'Error: Must be on linux server to use this function'
    here = os.getcwd()
    usr = os.getlogin()
    try:
        os.chdir("/home/"+usr)
        try:
            with open('.odbc.ini', 'r') as f:
                txt = f.read()
                lst = [i+1 for i in range(len(txt)) if txt[i]=='[']
                lst2 = [i for i in range(len(txt)) if txt[i]==']']
                return [txt[i:j] for i,j in zip(lst,lst2)][1:-1]
        except Exception as err:
            os.chdir(here)
            print str(err) + '\n use the create_odbc_ini function'
        os.chdir(here)
    except Exception as err:
        os.chdir(here)
        print str(err)

def get_db_list():
    """returns list of data bases"""
    if os.name != 'posix':
        return 'must be on riley'
    here = os.getcwd()
    usr = os.getlogin()
    try:
        os.chdir("/home/"+usr)
        try:
            with open('.odbc.ini', 'r') as f:
                txt = f.read()
                lst = [i for i in range(len(txt)-8) if txt[i:i+8]=='Database']
                return [txt[i+11:i+txt[i:].find('\n')] for i in lst]
        except Exception as err:
            os.chdir(here)
            print str(err) + '\n use the create_odbc_ini function'
        os.chdir(here)
    except Exception as err:
        os.chdir(here)
        print str(err)

def _sql_dtypes(value, char_limit = 255):
    """
    convert python objects to sqlalchemy objects

    Parameters
    ----------
    value : object
        a datapoint to be converted
    char_limit : int
        the number of characters you wish in varchar
    """
    if isinstance(value, int):
        return Integer
    if isinstance(value, float):
        return Float
    if isinstance(value, long):        
        return BigInteger
    if isinstance(value, bool):
        return Boolean
    if isinstance(value, str):
        return String(char_limit)
    if isinstance(value, datetime.date):
        return Date
    if isinstance(value, datetime.datetime):
        return DateTime
    else:
        return Text

class LegacySQL:

    def __init__(self):
        pass
    
    def q(self, query, dsn):
        """
        read data from SQL Server into pandas DataFrame

        Parameters
        ----------
        query : string 
            SQL statement you wish to execute
        dsn : string, default = None
            specifies which database to connect to, comes from user's odbc.ini file

        Returns
        -------
        res : pandas.DataFrame
            results from executing `query`
        """
        cnxn = pyodbc.connect(Trusted_Connection='yes', dsn = dsn)
        res = psql.read_frame(query,cnxn)
        cnxn.close()
        return res

    def table_write(self, df, table_name, dsn=None, if_exists='append'):
        """
        write to a table in SQL Server

        Parameters
        ----------
        df : pandas.DataFrame
            the pandas dataframe you want to write
        table_name : string
            the name of the table you to want to create
        dsn : string, default = None
            specifices with database to connect to, comes from user's odbc.ini file
        if_exists : {'fail', 'replace', 'append'} default = 'append'
            fail: If table exists, do nothing.
            replace: If table exists, drop it, recreate it, and insert data.
            append: If table exists, insert data. Create if does not exist.

        Returns
        -------
        None

        """    
        cnxn = pyodbc.connect(Trusted_Connection='yes', dsn=dsn)
        
        if if_exists not in ('append','replace','fail'):
            raise ValueError("'%s' is not a valid if_exists argument." % if_exists)
                             
        exists = LegacySQL.table_exists(self, table_name, cnxn)
        if if_exists == 'fail' and exists:
            raise ValueError("Table '%s' already exists." % table_name)
                             
        create = None
        if exists:
            if if_exists == 'fail':
                raise ValueError("Table '%s' already exists." % table_name)
            elif if_exists == 'replace':
                cur = cnxn.cursor()
                cur.execute("DROP TABLE %s;" % table_name)
                cur.close()
                create = LegacySQL.create_tableq(self, df, table_name)
        else:
            create = LegacySQL.create_tableq(self, df, table_name)
            
        if create is not None:
            cur = cnxn.cursor()
            cur.execute(create)
            cur.close()
        
        cur = cnxn.cursor()
        
        names = [s.replace(' ', '_').strip() for s in df.columns]
        LegacySQL._write_mssql(self, df, table_name, names, cur)
        cur.close()
        cnxn.commit()
        cnxn.close()

    def create_tableq(self, df, table_name):
        """
        cretes a SQL Server 'CREATE TABLE' query from a pandas DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            the pandas dataframe you want to write
        table_name : string
            the name of the table you to want to create

        Returns
        -------
        create_statement : string
            a SQL 'CREATE TABLE' query

        """
        safe_columns = [s.replace(' ', '_').strip() for s in df.dtypes.index]
        data_types = [LegacySQL.mssql_datatypes(self, dtype.type) for dtype in df.dtypes]
        column_types = zip(safe_columns, data_types)
        columns = ',\n  '.join('%s %s' % x for x in column_types)
        
        template =  """CREATE TABLE %(table_name)s (
                      %(columns)s
                      );"""
        
        create_statement = template % {'table_name': table_name, 'columns': columns}
        return create_statement


    def mssql_datatypes(self, col_type):
        """maps pandas datatype to mssql datatypes"""
        sqltype = 'varchar(2550)'
        
        if issubclass(col_type, np.floating):
            sqltype = 'real'
            
        if issubclass(col_type, np.integer):
            sqltype = 'bigint'
            
        if issubclass(col_type, np.datetime64) or col_type is datetime:
            sqltype = 'datetime'
            
        if col_type is datetime.date:
            sqltype = 'date'
            
        if issubclass(col_type, np.bool_):
            sqltype = 'bool'
            
        return sqltype

    def table_exists(self, table_name, cnxn):
        """
        Returns a boolean for existance of a table

        Parameters
        ----------
        table_name : string
            name of table to check existence of
        cnxn : pyodbc.connect
            pyodbc connection instance to check

        Returns
        -------
        bool for table existence

        """
        query = 'Select Top 1 * from %s' % table_name
        
        try:
            res = psql.read_frame(query,cnxn)
            return True
        except:
            #TODO read_frame error will print, need to fix
            return False

    def _write_mssql(self, df, table_name, names, cur):
        """writes a pandas dataframe to SQL Server
            NOT TO BE USED OUTSIDE OF table_write
        """
        bracketed_names = ['[' + column + ']' for column in names]
        col_names = ','.join(bracketed_names)
        wildcards = ','.join(['?'] * len(names))
        insert_query = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, col_names, wildcards)
        data = [tuple(x) for x in df.values]
        cur.executemany(insert_query, data)
