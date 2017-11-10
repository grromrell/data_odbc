from __future__ import print_function

import getpass
import pandas as pd
from datetime import datetime, date

from sqlalchemy.sql import select
from sqlalchemy.pool import QueuePool
from sqlalchemy import MetaData, Table, Column, create_engine
from sqlalchemy.types import Integer, Float, BigInteger, Boolean, String, Date, DateTime, Text

class Sql:

    def __init__(self, db_sys='mssql', dsn=None, db=None, host=None, port=None,
                 ad=False, trusted=False, domain=None, uid=None, pwd=None):
        """
        a class to support reading and writing data from databases, and pulling
        the resulting files into dictionaries or pandas dataframes

        Parameters
        ----------
        db_sys : ['mssql', 'sqlite', 'vertica', 'redshift']
            Specifies the RDBMS you want to use, use mssql for Azure databases
        
        dsn : str
            Data source name. If specified connection information will be
            looked up in your odbc.ini
        
        db : str
            If not using a dsn, the database to connect to

        host : str
            If not using a dsn, the hostname for the database connection

        port: str
            If not using a dsn, the port for the database connection

        ad : bool, default = True
            Whether to use Active Directory Authentication or not

        trusted : bool, default = False
            Indicates whether the database connection should be made with current
            credentials or be user supplied.

        domain : str
            Domain to connect to if using Active Directory authentication

        uid : str
            Explicitly pass username

        pwd : str
            Explicitly pass password

        Methods
        -------
        See the functions below
        """
        if not dsn and not all((db, host, port)):
            raise ValueError("Must supply either dsn or db, host and port")

        #Get credentials if needed
        if trusted:
            pass
        else:
            if not uid:
                print("Username: ", end='')
                uid = input()
                if ad:
                    uid = domain + "\\" + uid
            if not pwd:
                pwd = getpass.getpass()
        
        #Get engine url based on database type
        if dsn:
            if db_sys == 'mssql':
                if trusted:
                    engine_url = 'mssql+pyodbc://{0}'.format(dsn) 
                else:
                    engine_url = 'mssql+pyodbc://{0}:{1}@{2}'.format(uid, pwd, dsn)
            elif db_sys == 'sqlite':
                engine_url = 'sqlite:///{0}'.format(dsn)
            elif db_sys == 'vertica':
                engine_url = 'vertica+pyodbc://{0}:{1}@{2}'.format(uid, pwd, dsn)
            elif db_sys == 'redshift':
                engine_url = 'redshift+psycopg2://{0}:{1}@{2}'.format(uid, pwd, dsn)
        
        else:
            if db_sys == 'mssql':
                if trusted:
                    engine_url = 'mssql+pyodbc://{0}:{1}/{2}'.format(host, port, db)
                else:
                    engine_url = 'mssql+pyodbc://{0}:{1}@{2}:{3}/{4]'.format(uid, 
                                                                             pwd, 
                                                                             host,
                                                                             port,
                                                                             db)
            elif db_sys == 'sqlite':
                engine_url = 'sqlite:///{0}'.format(host)
            elif db_sys == 'vertica':
                engine_url = 'vertica+pyodbc://{0}:{1}@{2}:{3}/{4}'.format(uid, 
                                                                           pwd,
                                                                           host,
                                                                           port,
                                                                           db)
            elif db_sys == 'redshift':
                engine_url = 'redshift+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(uid, 
                                                                              pwd, 
                                                                              host,
                                                                              port,
                                                                              db)
        

        creator = create_engine(engine_url).pool._creator
        engine = create_engine(engine_url, 
                               pool=QueuePool(creator, 
                                              reset_on_return='commit'))
        metadata = MetaData()
        metadata.reflect(bind=engine)

        self.dsn = dsn
        self.engine = engine
        self.metadata = metadata
        #TODO: Turn metadata into SQL navigator?

    def query(self, query):
        """
        Execute arbitrary SQL select queries and read the results into a 
        pandas dataframe. 

        Parameters
        ----------
        query : string 
            SQL statement you wish to execute

        Returns
        -------
        df : pandas.DataFrame
            results from executing `query`
        """
        result = self.engine.execute(query)
        
        #If not a select then return
        if not result._metadata:
            return

        keys = result._metadata.keys
        result_list = []

        for row in result:
            values = list(row)
            data = dict(zip(keys, values))
            result_list.append(data)

        df = pd.DataFrame(result_list)       
        return df

    def import_table(self, table_name, output='dict', index_name=None):
        """
        import a table from the database into either a list of dictionaries or 
        a pandas dataframe

        Parameters
        ----------
        table_name : string
            name of the table you wish to access
        output : {'dict', 'df'}, default = dict
            specifies the output of the operation, whether a list of
            dictionaries or a pandas dataframe
        index_name : string
            name of the column where your index resides
        """
        conn = (self.engine).connect()
        table = (self.metadata).tables[table_name]
        s = select([table])
        result = conn.execute(s)
        keys = result._metadata.keys
        result_list = []
        
        #TODO: Add datatype in dataframes
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
        """
        if isinstance(data, pd.DataFrame):
            data.fillna('', inplace=True)
            data = Sql.df2dict(self, data)

        table = (self.metadata).tables[table_name]
        ins = table.insert()

        with self.engine.begin() as conn:
            conn.execute(ins, data)
    
    def write_table(self, data, table_name, if_exists='append', 
                    create=False, index=False, char_limit=255):
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
        Permanently drops a table
        
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

def _sql_dtypes(value, char_limit=255):
    """
    Convert python objects to sqlalchemy objects
    
    TODO: update this function to better match practice and allow
    custom datatypes and formating
    
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
    if isinstance(value, date):
        return Date
    if isinstance(value, datetime):
        return DateTime
    else:
        return Text

#TODO: Add helper classes for managing dsns?
