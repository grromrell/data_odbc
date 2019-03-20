from __future__ import print_function

import getpass
import pandas as pd
from datetime import datetime, date

from sqlalchemy.pool import QueuePool
from sqlalchemy.sql import select, null
from sqlalchemy import MetaData, Table, Column, create_engine
from sqlalchemy.types import Integer, Float, BigInteger, Boolean, String, Date, DateTime, Text

class Sql:

    def __init__(self, db_sys='postgres', dsn=None, db=None, host=None, port=None,
                 ad=False, trusted=False, domain=None, uid=None, pwd=None, schema=None,
                 driver=None, skip_reflect=False):
        """
        a class to support reading and writing data from databases, and pulling
        the resulting files into dictionaries or pandas dataframes

        Parameters
        ----------
        db_sys : ['mssql', 'sqlite', 'vertica', 'redshift', 'postgres']
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

        ad : bool, default = False
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

        schema : str
            Schema to use for connection. Only needed for postgres and its dialects.

        driver : str
            Occasionally a driver must be explicitly passed, mostly for MSSQL

        skip_reflect : bool
            Whether metadata should be reflected, skip if there are complex indices in your db.
            Some functions will not work with this turned on.

        Methods
        -------
        See the functions below
        """
        # get credentials if needed
        self.uid = uid
        self.__pwd = pwd
        self.trusted = trusted
        if not self.trusted:
            self._get_creds(ad)
    
        # for automatic connections
        self.dsn = dsn

        # for explicit connections
        self.db = db
        self.host = host
        self.port = port
        self.schema = schema
        self.driver = driver

        # find correct engine specification
        self.db_sys = db_sys
        self.skip_reflect = skip_reflect
        if self.dsn:
            engine_url = self._dsn_url()
        elif all((db, host, port)):
            engine_url = self._var_url()
        else:
            raise ValueError("Must supply either dsn or db, host and port") 

        # connect using engine
        self._connect(engine_url, skip_reflect)

    def _get_creds(self, ad):
        if not self.uid:
            self.uid = input("DB Username:\n")
            if ad:
                self.uid = domain + "\\" + uid
        if not self.__pwd:
            self.__pwd = getpass.getpass("DB Password:\n")

    def _dsn_url(self):
        """Construct engine url for dsn based connections"""
        if self.db_sys == 'mssql':
            if self.trusted:
                engine_url = 'mssql+pyodbc://{0}'.format(self.dsn) 
            else:
                engine_url = 'mssql+pyodbc://{0}:{1}@{2}'.format(self.uid, self.__pwd, self.dsn)
        elif self.db_sys == 'sqlite':
            engine_url = 'sqlite:///{0}'.format(self.dsn)
        elif self.db_sys == 'vertica':
            engine_url = 'vertica+pyodbc://{0}:{1}@{2}'.format(self.uid, self.__pwd, self.dsn)
        elif self.db_sys == 'postgres':
            engine_url = 'postgresql+psycopg2://{0}:{1}@{2}'.format(self.uid, self.__pwd, self.dsn)
        elif self.db_sys == 'redshift':
            engine_url = 'redshift+psycopg2://{0}:{1}@{2}'.format(self.uid, self.__pwd, self.dsn)

        return engine_url

    def _var_url(self):
        """Construct engine url for expilictly based connections"""
        if self.db_sys == 'mssql':
            if self.trusted:
                engine_url = 'mssql+pyodbc://{0}:{1}/{2}'.format(self.host, 
                                                                 self.port, 
                                                                 self.db)
            else:
                base_url = 'mssql+pyodbc://{0}:{1}@{2}:{3}/{4}'

                # add in driver if needed to pass explicitly
                if self.driver:
                    self.driver = self.driver.replace(' ','+')
                    base_url += '?driver={0}'.format(self.driver)

                
                engine_url = base_url.format(self.uid, 
                                             self.__pwd, 
                                             self.host,
                                             self.port,
                                             self.db)
        elif self.db_sys == 'sqlite':
            engine_url = 'sqlite:///{0}'.format(self.host)
        
        elif self.db_sys == 'vertica':
            engine_url = 'vertica+pyodbc://{0}:{1}@{2}:{3}/{4}'.format(self.uid, 
                                                                       self.__pwd,
                                                                       self.host,
                                                                       self.port,
                                                                       self.db)
        elif self.db_sys == 'postgres':
            engine_url = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(self.uid,
                                                                            self.__pwd,
                                                                            self.host,
                                                                            self.port,
                                                                            self.db)
    
        elif self.db_sys == 'redshift':
            engine_url = 'redshift+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(self.uid, 
                                                                          self.__pwd, 
                                                                          self.host,
                                                                          self.port,
                                                                          self.db)

        return engine_url

    def _connect(self, engine_url, skip_reflect=False):
        """Connect to database based on engine url"""
        creator = create_engine(engine_url).pool._creator
        engine = create_engine(engine_url, 
                               pool=QueuePool(creator, 
                               reset_on_return='commit'))

        self.skip_reflect = skip_reflect
        if not skip_reflect:
            metadata = MetaData(schema=self.schema)
            metadata.reflect(bind=engine)
            self.metadata = metadata

        self.engine = engine

    def _refresh(self):
        """Refresh database connection"""
        if not self.skip_reflect:
            self.metadata.reflect(bind=self.engine)

    def change_db(self, db, skip_reflect=False):
        """
        Change database without reconnecting, uses originally passed 
        credentials.

        Parameters
        ----------
        db : string
            Database to connect to
        """
        self.db = db
        self._connect(self._var_url(), skip_reflect)

    def change_schema(self, schema, skip_reflect=False):
        """
        Change schema without reconnecting, using originally passed 
        credentials. Used mostly for postgres

        Parameters
        ----------
        schema : str
            Schema to switch context to
        """
        self.schema = schema
        self._connect(self._var_url(), skip_reflect)

    def query(self, query, commit=False):
        """
        Execute arbitrary SQL queries and read the results into a 
        pandas dataframe. 

        Parameters
        ----------
        query : string 
            SQL statement you wish to execute
        commit : bool
            Whether to exit transaction before running query

        Returns
        -------
        df : pandas.DataFrame
            results from executing `query`
        """
        conn = self.engine.connect()
        if commit:
            conn.execute("commit")

        result = conn.execute(query)

        #If not a select then return
        if not result._metadata:
            conn.close()
            self._refresh()
            return

        keys = result._metadata.keys
        result_list = []

        for row in result:
            values = list(row)
            data = dict(zip(keys, values))
            result_list.append(data)
        df = pd.DataFrame(result_list)
        
        conn.close()
        self._refresh()
        return df
    
    def lazy_query(self, query):
        """
        Execute arbitrary SQL queries and lazily read the results

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
        for row in result:
            values = list(row)
            data = dict(zip(keys, values))
            yield data
        
        result.close()
        self._refresh()

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

        if self.schema:
            table_name = self.schema + '.' + table_name

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
        Inserts data into a SQL table, can be used iteratively or as a batch

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
            data = data.astype(object).where((pd.notnull(data)), None)
            data = Sql.df2dict(self, data)

        if self.schema:
            table_name = self.schema + '.' + table_name

        table = (self.metadata).tables[table_name]
        ins = table.insert()

        with self.engine.begin() as conn:
            conn.execute(ins, data)

        self._refresh()
    
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
        
        if self.schema:
            table_name = self.schema + '.' + table_name

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
                Sql.insert(self, table_name, data)
                return

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
            Sql.insert(self, table_name, data)
        
        self._refresh()

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
        if self.schema:
            table_name = self.schema + '.' + table_name
        table = (self.metadata).tables[table_name]
        table.drop(self.engine)
        self._refresh()

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
        return BigInteger
    if isinstance(value, float):
        return Float
    if isinstance(value, bool):
        return Boolean
    if isinstance(value, str):
        return String(char_limit)
    if isinstance(value, datetime):
        return DateTime
    else:
        return Text

#TODO: Add helper classes for managing dsns?
