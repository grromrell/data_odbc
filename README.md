---------
data_odbc
---------

This module makes it easier to read and write data to databases (primarily MS SQL Server and SQLite3 at the moment) 
from Python using the Pandas Data Analysis Toolkit.

--------
Tutorial
--------

Note: These steps are only required or MSSQL. If you are using SQLite3 skip to step <TODO>

    1. Setting up your connections:

        The first thing you will need to do is set up a place to store your ODBC connections. If you are on a Linux 
        machine all you need to do is:

            $ ipython
            In [1]: import data_odbc as do
            In [2]: do.create_odbc_ini()

        This will create a (hidden) file in your home directory that can store all of your connections called odbc.ini. 
        Once your odbc.ini file is created you can then create individual SQL server connections called Data Source Names
        (DSNs). The process to do so is simple:

            In [3]: do.add_dsn('dsn_name', 'server_location', 'database_name')
            
            Note: This process relies on a odbc manager (most people use unixODBC) and the FreeTDS driver. It may in
                  future included support for other drivers (specifically Microsoft's native driver). The driver
                  must be stored in /usr/local/lib. Sometimes FreeTDS likes to install in /usr/lib/x86_64-linux-gnu/
                  odbc/ (the x86_64 bit depends on your architecture). If that is the case create a symlink into the 
                  proper directory with a command like: 
                    
                    $ sudo ln -s /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so /usr/local/lib/libtdsodbc.so

        This command will store a DSN in your odbc.ini files that save the connection to the database you specified.
        The first thing you input (in this example 'dsn_name' will be the name you use to call the database later, 
        so make it easy to remember and sufficiently unique.You have now created your first connection! It is now 
        possible to query databases, among other things coming up later. In case you forget what DSNs or Databases 
        you are connected to you can see them both through simple commands:

            In [4]: do.get_dsn_list()
            Out[4]: ['dsn_name']

            In [5]: do.get_db_list()
            Out[5]: ['database_name']

        If you are on a Windows or OSX machine the process is slighty harder. In future this package *may* support 
        non-Linux set-ups but for now you will have to create and check your DSNs manually. Both Windows and OSX have 
        GUIs that should make the process easier.
            
            See here for PC: http://blog.mclaughlinsoftware.com/2012/09/12/sql-server-odbc-osn/
            See here for OSX: http://www.actualtech.com/readme.php

TODO: These steps are outdated (rely on pydobc directly), need to updated package and README with SQLAlchemy 
      functionality.

    2. Querying the database:

        From this point forward the queries should work no matter what type of OS you are running as long as you have set
        up your DSNs correctly. The first thing you need to do is start a connection by calling the 'Sql' class. 
        Following that all the commands are available. Querying is the main function of this module. To make a query 
        call the 'query' function:

            In [6]: my_conn = do.Sql(dsn = 'dsn_name')
            In [7]: my_data = my_conn.query('Select * From table_name')
            
        TODO: executing arbitrary queries and normal query structure

    3. Making changes to the database:
        
        TODO: Insert into tables and create tables
        
Contact: Greg Romrell (grromrell@gmail.com) or Ryan Brunt (rjhbrunt on github)
