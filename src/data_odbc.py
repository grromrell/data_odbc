import os
import datetime
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, create_engine, text
from sqlalchemy.sql import select
from sqlalchemy.types import Integer, Float, BigInteger, Boolean, String, Date, DateTime, Text

class Sql:

    def __init__(self, dsn, db = 'mssql'):
        """
        a class to support reading and writing data from SQL Server

        Parameters
        ----------
        dsn : string
            specifies which database to connect to. For mssql connections the
            dsn is the name specified in the user's odbc.ini file. For sqlite
            the dsn is the absolute location of the .db file 
            (i.e.: /home/user.myd.db)

        db : ['mssql', 'sqlite'], default = 'mssql'
            specifies the RDBMS to be used
        """
        if db == 'mssql':
            engine_url = 'mssql+pyodbc://%s' % dsn 
        elif db == 'sqlite':
            engine_url = 'sqlite:///%s' % dsn
        else:
            raise ValueError("%s is not a valid dsn") % db

        engine = create_engine(engine_url)
        metadata = MetaData()
        metadata.reflect(bind = engine)

        self.dsn = dsn
        self.engine = engine
        self.metadata = metadata

    def query(self, query, select=False):
        """
        execute arbitrary SQL queries, especially for functions where no
        builtin exists. If executing a select, insert or create table
        statement, it is recommended you use the provided method. If a select
        statement is too complicated for the select_table function then it
        will parsed here put into a pandas DataFrame (if possible), remeber
        to pass the select parameter as True if this is the case. If you run
        into trouble using this command consult the SQL Alchmey docs on using
        the text() command: http://bit.ly/1iyDGY2

        Parameters
        ----------
        query : string 
            SQL statement you wish to execute

        select : bool, default = False
            True if a select statement, false otherwise

        Returns
        -------
        df : pandas.DataFrame
            If applicable
        """
        conn = (self.engine).connect()
        s = text(query)
        result = conn.execute(s)

        if select:   
            keys = result._metadata.keys
            result_list = []

            for row in result:
                values = list(row)
                data = dict(zip(keys, values))
                result_list.append(data)

            df = pd.DataFrame(result_list)
            return df

    def select_table(self, table_name, output = 'df', index_name = None):
        """
        import a table from a database into either a pandas dataframe or a 
        list of dictionaries. Currently only supports 'Select *' like behavior.

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
        df : pandas.DataFrame
            the outcome of your select statement when the output parameter is
            'df'

        result_list : list of dictionaries
            the outcome of your select statement when the output parameter is
            'dict'
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
        inserts data into a database, can be used iteratively or as a batch

        Parameters
        ----------
        table_name : string
            name of the table you wish to acces
        data : list of dictionaries
            a list containing the dictionaries where your data resides,
            the keys in these dictionaries must line up with the column
            names in your table and must all be the same. Can also be a 
            Pandas DataFrame that will be converted.
        """
        if type(data) == 'pandas.core.frame.DataFrame':
            data = Sql.df2dict(self, data)

        table = (self.metadata).tables[table_name]
        ins = table.insert()
        conn = (self.engine).connect()
        conn.execute(ins, data)
        conn.close()
    
    def write_table(self, data, table_name, if_exists = 'append', 
                    create = False, index = False, char_limit = 255):
        """
        Allows the user to write data to a database directly with a completed
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
        Permanently drops a table from a database
        
        Parameters
        --------
        table_name : string
            the name of the table you wish to drop
        """
        table = (self.metadata).tables[table_name]
        table.drop(self.engine)
        (self.metadata).remove((self.metadata).tables[table_name])

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
        list of dictionaries
        """
        return [dict((k, v) for k,v in zip(self.columns, row)) for row in 
                self.values] 

    def dict2df(self, data):
        """
        Converts lists of dictionaries to pandas dataframe for convenient 
        table writing

        Parameters
        ----------
        data : list of dictionaries or dictionary
            data that is to be converted to dataframe. The keys of the
            dictionary must be the equivalent column names used in your
            database table. If using only number values an index error will
            occur.

        Returns
        -------
        pandas dataframe
        """
        return pd.DataFrame(data)

def create_odbc_ini():
    """
    Creates an odbc.ini file which stores user specified dsn's
    """
    here = os.getcwd()
    usr = os.getlogin()
    if os.name != 'posix':
        raise ValueError("Must be running Linux to make an odbc.ini file")
    elif os.path.exists(os.path.abspath('.odbc.ini')):
        raise ValueError("odbc.ini already exists")
    else:
        try:
            os.chdir("/home/"+usr)
            with open('.odbc.ini', 'w') as f:
                #TODO: Add support for other drivers
                f.write("[Data Sources]\n\n[Default]\nDriver = /usr/local/lib/libtdsodbc.so\n")
            os.chdir(here)
        except Exception as err:
            os.chdir(here)
            print str(err)

def add_dsn(dsn, server, db):
    """
    Adds new dsn to odbc.ini

    Parameters
    ----------
    dsn : string
        User specified name for connection
    server : string
        Server location where database is kept
    db : string 
        Name of database
    """
    if os.name != 'posix':
        raise ValueError("Must be running Linux to add a connection with this function")
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
    """
    Returns a list of the dsns from the user's odbc.ini file
    """
    if os.name != 'posix':
        raise ValueError('Must be running Linux to return connections with this function')
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
    """
    Returns a list of the databases with existing dsns
    """
    if os.name != 'posix':
        raise ValueError('Must be running Linux to return databases with this function')
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
    Convert python objects to sqlalchemy objects

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

"""
Copyright (C) 2014 Greg Romrell and Ryan Brunt

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
