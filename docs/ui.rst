ConDB Command Line Interface
============================

List of Commands
----------------

    * create - create folder
    * write - write data directly to the database
    * read - read data directly from the database
    * put - send data to the ConDB web server
    * get - get data from the ConDB web server
    * tag - tag a database state

Commands
--------

create
~~~~~~

``create`` command is used to create a new folder in the existing ConDB database

.. code-block:: shell

    condb create [options] <database name> <folder_name> <column>:<type> [...]
  
      Options:
          -h <host>
          -p <port>
          -U <user>
          -w <password>
  
          -c - force create, drop existing table
          -s - just print SQL needed to create the table without actually creating anything
          -o <table owner>
          -R <user>,... - DB users to grant read permissions to
          -W <user>,... - DB users to grant write permissions to

write
~~~~~

``write`` command is used to write data directly into an existing folder of an existing ConDB database.

.. code-block:: shell

    condb write [options] <database name> <folder_name> < <CSV file>
  
      Options:
          -h <host>
          -p <port>
          -U <user>
          -w <password>
  
          -d <data_type>              default = blank

read
~~~~

The command reads data directly from an existing folder of an existing ConDB database.

.. code-block:: shell

    condb write [options] <database name> <folder_name> < <CSV file>
  
      Options:
          -h <host>
          -p <port>
          -U <user>
          -w <password>
  
          -d <data_type>              default = blank

put
~~~

Reads data as a CSV files from stdin and sends data to a ConDB web server

.. code-block:: shell

    condb put [options] <folder_name> < <CSV file>
  
      Options:
          -s <server URL>             CONDB_SERVER_URL envirinment variable can be used too
          -U <username>
          -w <password>
          -d <data type>

get
~~~

Reveives data from a ConDB web server and prints it in CSV format

.. code-block:: shell

    condb get [options] <folder_name>
  
      Options:
          -s <server URL>             CONDB_SERVER_URL envirinment variable can be used too
          -t <time>                   Tv, numeric or ISO format (YYYY-MM-DD hh:mm:ss), default = now
          -t <time0>-<time1>          Tv range, numeric or ISO
          -T <tag>    
          -d <data_type>              default = blank
          -c <channel>                single channel
          -c <channel>-<channel>      channel range

tag
~~~

Associates a text tag with a state of the ConDB database

.. code-block:: shell

    condb tag [options] <folder_name> <tag name>
  
      Options:
          -s <server URL>             CONDB_SERVER_URL envirinment variable can be used too
          -U <username>
          -w <password>
          -r <tr>                     optional Tr, default=now
          -T <existing tag>           existing tag to copy
          -f                          move the tag to new Tr if exists
