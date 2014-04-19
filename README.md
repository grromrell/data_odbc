data_odbc
---------

This module makes it easier to read and write data to databases (primarily MS SQL Server and SQLite3 at the moment) 
from Python using the Pandas Data Analysis Toolkit.

Tutorial
--------

Note: These steps are only required for MSSQL. If you are using SQLite3 skip to Step 2.

    1. Setting up your connections:

        The first thing to do is set up a place to store your ODBC connections. If you are on a 
        Linux machine all you need to do is:

            $ ipython
            In [1]: import data_odbc as do
            In [2]: do.create_odbc_ini()

        This will create a (hidden) file in your home directory that can store all of your connections 
        called odbc.ini. Once your odbc.ini file is created you can then create individual connections 
        called Data Source Names(DSNs). The process to do so is simple:

            In [3]: do.add_dsn('dsn_name', 'server_location', 'database_name')
            
            Note: This process relies on a odbc manager (most people use unixODBC) and the FreeTDS 
                  driver. It may in future included support for other drivers (specifically Microsoft's 
                  native driver). The driver must be stored in /usr/local/lib. Sometimes FreeTDS likes 
                  to install in /usr/lib/x86_64-linux-gnu/odbc/ (the x86_64 bit depends on your 
                  architecture). If that is the case create a symlink into the proper directory with 
                  a command like: 
                    
                  $ sudo ln -s /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so /usr/local/lib/libtdsodbc.so

        This command will store a DSN in your odbc.ini files that save the connection to the database you
        specified. The first thing you input (in this example 'dsn_name' will be the name you use to call 
        the database later, so make it easy to remember and sufficiently unique. You have now created your
        first connection! It is now possible to query databases. In case you forget what DSNs or Databases 
        you are connected to you can see them both through simple commands:

            In [4]: do.get_dsn_list()
            Out[4]: ['dsn_name']

            In [5]: do.get_db_list()
            Out[5]: ['database_name']

        If you are on a Windows or OSX machine the process is slighty harder. In future this package 
        *may* support non-Linux set-ups but for now you will have to create and check your DSNs 
        manually. Both Windows and OSX have GUIs that should make the process easier.
            
            See here for PC: http://blog.mclaughlinsoftware.com/2012/09/12/sql-server-odbc-osn/
            See here for OSX: http://www.actualtech.com/readme.php

    2. Selecting Data:

        All of the functions of this modul are preceded by calling the 'Sql' class. There are two ways to 
        select data from your database. The first is by using the select_table() method:

            In [6]: conn = do.Sql(dsn = 'dsn_name')
            In [7]: data = conn.select_table(table_name='table_name', output='df', index_name='id_col')
            
        This will return a pandas dataframe selecting all of the columns from table 'table_name' and adding 
        the primary key as the index. The other way to select data is to user the query() method:
        
            In [8]: query = """Select Column1,
                                      Column2,
                                      Column3
                                      From table_name
                                      Where Column1 >= 1 and Column2 is not null"""
                                      
            In [9]: data = conn.query(query=query, select=True)
            
        The query method can also be used to execute any arbitrary SQL queries while staying back-end 
        agnostic thanks to SQL Alchemy's text() method. If you have trouble using this command read about 
        the text method in the tutorial: http://bit.ly/1iyDGY2
            
    3. Making changes to a database:
        
        There are three supported methods of changing the contents of a database in data_odbc: creating a
        table, dropping a table and inserting into a table. An example of each is found below. If you want
        to execute a query that cannot be resolved with the built-in methods use the query() method.
        
            In [10]: #create_table: conn.write_table(data_to_write, table_name,
                                                     if_exists='append', create=True)
            In [11]: #create_and_write_table: conn.write_table(data_to_write, table_name, 
                                                               if_exists='fail', create=False)
            In [12]: #insert: conn.write_table(data_to_write, table_name, if _exists='append', create=False)
            In [13]: #another_insert: conn.insert(table_name, data_to_write)
            In [14]: #drop_table: conn.drop_table(table_name)
        
Contact: Greg Romrell (grromrell@gmail.com) or Ryan Brunt (rjhbrunt on github)
