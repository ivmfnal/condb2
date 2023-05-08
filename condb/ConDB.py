import sys, time, datetime
import io
from .dbdig import DbDig

#from dbdig import DbDig

def cursor_iterator(c):
    tup = c.fetchone()
    while tup:
        yield tup
        tup = c.fetchone()

class ConDB:
    def __init__(self, connection=None, connstr=None):
        """Initializes the connection to the ConDB
        
        Parameters
        ----------
        connection:
            Psycopg2 connection object
        connstr : str
            Postgres connection string, e.g. "host=... port=... dbname=...". 
            Ignored if ``connection`` is provided
        """
        if isinstance(connection, str):
            connstr = connection
            connection = None
        self.Conn = connection
        self.ConnStr = connstr
        
    def connect(self):
        if self.Conn == None:
            import psycopg2
            self.Conn = psycopg2.connect(self.ConnStr)
        return self.Conn
    
    def cursor(self):
        conn = self.connect()
        return conn.cursor()
        
    def openFolder(self, name):
        """Opens existing ConDB folder.
        
        Parameters
        ----------
            name : str
                Name of the folder to open
        
        Returns
        -------
        CDFolder or None
            If the folder does not exist, returns None. Otherwise - CDFolder object representing the folder
        """

        return CDFolder.open(self, name)
        
    def namespaces(self):
        dig = DbDig(self.Conn)
        return dig.nspaces()

    def createFolder(self, name, column_types, owner=None, grants = {}, drop_existing=False):
        """Creates or opens ConDB folder
        
        Parameters
        ----------
            name : str
                Folder name. May include namespace (schema), e.g.: "my_namespace.my_folder". If namespace is not given, use "public" namespace
            column_types : list
                List of tuples: (column_name, data_type). column_name is a valid Postgres column name. data_type is a Postgres data type
                e.g. "int" or "double precision" or "text"
            grants : dict
                Dictionary with 2 optional keys: 'r' and 'w'. Each key, if present has a value of a list of strings, each item is the
                database username to grant "read" or "write" permissions to when the folder is created.
            owner : str
                Owner for folder tables if the folder is to be created
            drop_existing : boolean
                If the folder already exists and drop_existing is True, then existing folder will be dropped and data will be discarded.
                If the folder already exists and drop_existing is False, the existing folder will be opened instead of creating new folder.
        """
        t = CDFolder.create(self, name, column_types, owner, grants, drop_existing)
        return t

    def execute(self, table, sql, args=()):
        #print "DB.execute(%s, %s, %s)" % (table, sql, args)
        table_no_ns = table.split('.')[-1]
        sql = sql.replace('%t', table)
        sql = sql.replace('%T', table_no_ns)
        c = self.cursor()
        c.execute(sql, args)
        return c

    def copy_from(self, title, data, table_template, columns):
        table = table_template.replace('%t', title)
        c = self.cursor()
        try:
            c.copy_from(data, table, columns=columns)
        except:
            c.execute("rollback")
            raise
        else:
            c.execute("commit")
        return c
        
    def disconnect(self):
        if self.Conn:   self.Conn.close()
        self.Conn = None

    def tables(self, namespace = "public"):
        dig = DbDig(self.Conn)
        db_tables = dig.tables(namespace) or []
        db_tables_set = set(db_tables)
        db_tables.sort()
        condb_tables = []

        for t in db_tables:
            if t.endswith("_snapshot"):
                tn = t[:-len("_snapshot")]
                if (tn+"_update") in db_tables_set and (tn+"_tag") in db_tables_set:
                        t = "%s.%s" % (namespace, tn)
                        t = self.tableFromDB(t)
                        if t:   condb_tables.append(t)
        return condb_tables

class CDFolder:

    CreateTables = """
        create table %t_tag
        (
            __name        text    primary key,
            __tr          double precision,
            __created     timestamp with time zone default current_timestamp,
            __comment     text    default ''
        );

        create table %t_update
        (
            __tv                      double precision,
            __tr                      double precision,   
            __channel                 bigint,
            __data_type               text,
            %d
            ,
            primary key (__tv, __tr, __channel, __data_type)
        );

        create index %t_update_tr on %t_update(__tr);
        create index %t_update_data_type on %t_update(__data_type);
    """


    DropTables = """
        drop table if exists %t_tag;
        drop table if exists %t_update;
    """

    StructureColumns = ["__channel", "__tv", "__tr", "__data_type"]
    
    def __init__(self, db, name, data_columns_types=None):
        self.Name = name
        self.DataColumnsTypes = data_columns_types
        self.AllColumns = None
        if data_columns_types is not None:
            self.DataColumns = [name for name, _ in data_columns_types]
            self.AllColumns = self.StructureColumns + self.DataColumns
        self.DB = db
        words = name.split(".",1)
        if len(words) == 2:
            self.FolderName = words[1]
            self.Namespace = words[0]
        else:
            self.FolderName = words[0]
            self.Namespace = ""
            
    def readDataColumnsFromDB(self):
        dig = DbDig(self.DB.connect())
        words = self.Name.split('.')
        ns = 'public'
        name = self.Name
        if len(words) > 1:
            ns = words[0]
            name = words[1]
        columns = dig.columns(ns, self.Name + "_update")
        if not columns:
            raise ValueError("Not a conditions DB table (update table not found)")
        #print "readDataColumnsFromDB(%s): columns: %s" % (self.Name, columns)
        self.DataColumnsTypes = [(name, type) for name, type in columns if name not in self.StructureColumns]
        self.DataColumns = [name for name, _ in self.DataColumnsTypes]
        self.AllColumns = self.StructureColumns + self.DataColumns
        if not self.validate():
            raise ValueError("Not a conditions DB table (verification failed)")

    @staticmethod
    def open(db, full_name):
        dbcon = db.connect()
        dig = DbDig(dbcon)
        #print("dbcon:", dbcon)
        name = full_name
        ns = 'public'
        words = full_name.split('.')
        if len(words) > 1:
            ns = words[0]
            name = words[1]
        try:
            columns = dig.columns(ns, name + "_update")
        except:
            return None 
        if not columns:
            return None     # <name>_update table does not exist
        columns_types = [(tup[0], tup[1]) for tup in columns if tup[0] not in CDFolder.StructureColumns]
        return CDFolder(db, full_name, data_columns_types=columns_types)

    def data_column_types(self):
        """
        Returns
        -------
        list
            list of (name, type) tuples for the data columns
        """
        return self.DataColumnsTypes[:]

    def __columns(self, columns, prefix = None, as_text = False, exclude=[]):
        if exclude:
            columns = [c for c in columns if c not in exclude]
        if prefix:
            columns = [f"{prefix}.{c}" for c in columns]
        if as_text:
            return ",".join(columns)
        else:
            return columns

    def data_columns(self, prefix = None, as_text = False):
        return self.__columns(self.DataColumns, prefix=prefix, as_text=as_text)

    def all_columns(self, prefix = None, as_text = False):
        return self.__columns(self.AllColumns, prefix=prefix, as_text=as_text)

    def execute(self, sql, args=()):
        #print "Folder.execute(%s, %s)" % (sql, args)
        return self.DB.execute(self.Name, sql, args)

    def copy_from(self, data, table, columns):
        return self.DB.copy_from(self.Name, data, table, columns)

    def exists(self):
        from psycopg2.errors import UndefinedTable
        
        try:    self.execute("select * from %t_update limit 1")
        except UndefinedTable:
            return False
        else:
            return True
        finally:
            self.execute("rollback")

    @staticmethod
    def create(db, name, column_types, owner, grants = {}, drop_existing=False):
        """Static method to create a ConDB folder.
        
        Parameters
        ----------
            db : ConDB
                ConDB object
            the rest :
                same arguments as for ConDB.createFolder() method
        """
        f = CDFolder(db, name, column_types)
        f.createTables(owner, grants, drop_existing)
        return f

    def tableNames(self):
        return [self.Name + "_" + s for s in ("tag", "update")]

    def dataTableNames(self):
        #return [self.Name + "_" + s for s in ("snapshot_data", "update")]
        return [self.Name + "_" + s for s in ("update",)]

    def validate(self):
        # check if all necessary tables exist and have all the columns
        c = self.DB.cursor()
        for t in self.tableNames():
            try:    
                c.execute("select * from %s limit 1" % (t,))
            except: 
                c.execute("rollback")
                return False
        if self.Columns:
            columns = ','.join(self.Columns)
            for t in self.dataTableNames():
                try:    c.execute("select %s from %s limit 1" % (columns, t))
                except: 
                    c.execute("rollback")
                    return False
        return True
    
    @staticmethod
    def createSQL(name, column_types, owner = None, grants = {}, drop_existing=False):

        namespace = ""
        if '.' in name:
            namespace, name = name.split('.', 1)

        sql = ""
        if namespace:
            sql += f"\nset search_path to {namespace};\n"
        if drop_existing:
            sql += CDFolder.DropTables
        if owner is not None:
            sql += f"\nset role {owner};\n"
        
        columns = ",".join(["%s %s" % (n,t) for n,t in column_types])
        sql += CDFolder.CreateTables
        
        if grants:
            read_roles = ','.join(grants.get('r',[]))
            if read_roles:
                sql += f"""\ngrant select on 
                        %t_tag,
                        %t_update
                        to {read_roles};
                """
            write_roles = ','.join(grants.get('w',[]))
            if write_roles:
                sql += f"""\ngrant insert, delete, update on 
                        %t_tag,
                        %t_update
                        to {write_roles};
                """

        sql = sql.replace("%d", columns).replace("%t", name)
        #print("createSQL: sql:\n", sql)
        return sql
    
    def createTables(self, owner = None, grants = {}, drop_existing=False):
        from psycopg2.errors import UndefinedTable
        
        exists = self.exists()
        c = self.DB.cursor()
        
        if exists and drop_existing:
            try:    self.execute(self.DropTables)
            except UndefinedTable:
                c.execute("rollback")
            exists = False
        if not exists:
            c = self.DB.cursor()
            sql = CDFolder.createSQL(self.Name, self.DataColumnsTypes, drop_existing=drop_existing, owner=owner,
                        grants=grants)
            c.execute(sql)

    def tags(self):
        """Returns list of tags defined for the folder
        
        Returns
        -------
        generator
            generates list of tuples (name, tr) for existing tags
        """
        c = self.execute("""select __name, __tr from %t_tag order by __name""", ())
        return cursor_iterator(c)
        
    def dataTypes(self):
        """Returns list of data types defined for the folder
        
        Returns
        -------
            list
                list of strings
        """
        c = self.execute("""select distinct __data_type from %t_update order by __data_type""", ())
        return [x[0] for x in c.fetchall()]

    def split_update_tuple(self, tup):
        n_struct = len(self.StructureColumns)
        struct, data = tup[:n_struct], tup[n_struct:]
        return struct, data             # (__channel, __tv, __tr, __data_type), (data....)
        
    def shadow_data(self, data_iterator):
        # filter out hidden rows
        # assume rows are sorted by channel, tv, tr desc
        min_tr = None
        last_channel = None
        for tup in data_iterator:
            (channel, tv, tr, _), _ = self.split_update_tuple(tup)
            if channel != last_channel:
                last_channel = channel
                min_tr = tr
                yield tup
            elif tr >= min_tr:
                min_tr = tr
                yield tup

    def _get_data_point(self, tv, tag=None, tr=None, data_type=None, channel_range=None):
        # returns iterator [(channel, tv, data_type, data, ...)] unsorted
        # if data_type is None, returns all data types. otherwise - specified
        # data_type can be ""

        all_columns = self.all_columns(prefix="u", as_text=True)

        params = {
            "tv":   tv,
            "tag":  tag,
            "tr":   tr,
            "data_type": data_type,
            "min_channel": channel_range[0] if channel_range else None,
            "max_channel": channel_range[1] if channel_range else None
        }

        if tag is not None:
            c = self.execute(f"""
                select distinct on (u.__channel) {all_columns} from %t_update u, %t_tag t
                    where u.__tv <= %(tv)s
                        and u.__tr < t.__tr
                        and t.__name = %(tag)s
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tr desc, u.__tv desc
            """, params)
        else:
            c = self.execute(f"""
                select distinct on (u.__channel) {all_columns} from %t_update u
                    where u.__tv <= %(tv)s
                        and (%(tr)s is null or u.__tr < %(tr)s)
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tr desc, u.__tv desc
            """, params)
    
        yield from cursor_iterator(c)

    def merge_timelines(self, initial, timelines):
        return sorted(list(initial) + list(timelines), key = lambda row: tuple(row[:3]))       # sort by channel, tv, data_type

    def getData(self, t0, t1=None, tag=None, tr=None, data_type=None, channel_range=None):
        """Retieves data for specified validity time or time interval from the folder
        
        Parameters
        ----------
            t0 : float, int
                The beginning of the time interval.
            t1 : float, int or None
                The end of the time interval. For each channel, the output will include the most recent data values preceding t1
                and all the values between t1 and t2.
                If t0 == t1 or t1 == None, the method returns data for the t0 point in time
            tr : float, int
                Retieve data retrospectively from a previous state of the database specified with tr as a timestamp.
                The result will include only data added *before* the specified tr. By default, will include most recent data.
            tag : str
                Text tag previously assigned to a Tr value.
            data_type : str
                Data type to include. If None, will include data for all data types
            channel_range : tuple
                Tuple (min_channel, max_channel) if provided, only the channels within the specified interval, inclusively
                will be included in the output. Each one of the limits can be None, which means there is no limit.
        
        Returns
        -------
        generator
            Generator of tuples: (channel, tv, tr, data_type, <data column values>...)
        """

        # initial data
        initial = self._get_data_point(t0, tag=tag, tr=tr, data_type=data_type, channel_range=channel_range)
        if t0 == t1 or t1 is None:
            return initial
        
        all_columns = self.all_columns(prefix="u", as_text=True)

        params = {
            "tv0":   t0,
            "tv1":   t1,
            "tag":  tag,
            "tr":   tr,
            "data_type": data_type,
            "min_channel": channel_range[0] if channel_range else None,
            "max_channel": channel_range[1] if channel_range else None
        }
        
        if tag is not None:
            c = self.execute(f"""
                select distinct on (u.__channel, u.__tv) {all_columns} from %t_update u, %t_tag t
                    where u.__tv > %(tv0)s
                        and (%(tv1)s is null or u.__tv <= %(tv1)s)
                        and u.__tr < t.__tr
                        and t.__name = %(tag)s
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tv, u.__tr desc
            """, params)
        else:
            c = self.execute(f"""
                select distinct on (u.__channel, u.__tv) {all_columns} from %t_update u
                    where u.__tv > %(tv0)s
                        and (%(tv1)s is null or u.__tv <= %(tv1)s)
                        and (%(tr)s is null or u.__tr < %(tr)s)
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tv, u.__tr desc
            """, params)
        timelines = cursor_iterator(c)
        return self.shadow_data(self.merge_timelines(initial, timelines))

    def addData(self, data, data_type="", tr=None, columns=None):
        """Adds data to the folder
        
        Parameters
        ----------
            data : iterable
                Iterable with tuples: (channel, tv, <data values>, ...)
                channel is the integer channel number
                tv is numeric validity time (integer or floating point)
                data values are in the same order as the list of columns used when the folder was created
            data_type : str 
                Data type to associate with the data. Default - blank ""
            tr : float or int
                Tr to associate the data with. Bt default, current timestamp will be used as floating point number
            columns : list of strings
                Optional, names of data columns present in the input data, without channel amd tv. If not specified,
                the data is assumed to contain all the data columns
        
        Returns
        -------
        float
            Tr timestamp
        """
        # data: [(channel, tv, data, ...),...]
        csv_rows = []
        if tr is None:  tr = time.time()
        for tup in data:
            (channel, tv), payload = tup[:2], tup[2:]
            row = ["\\N" if x is None else str(x) for x in (channel, tv, tr, data_type) + tuple(payload)]
            csv_rows.append("\t".join(row))

        csv = io.StringIO('\n'.join(csv_rows))
        if columns is None:
            columns = self.all_columns()
        else:
            for c in columns:
                if c not in self.DataColumns:
                    raise ValueError("Unrecognized data column name")
            columns = self.StructureColumns + columns
        self.copy_from(csv, "%t_update", columns)
        return tr

    def tag(self, tag, comment="", override=False, tr=None):
        """Creates new tag with the specified Tr
        
        Parameters
        ----------
            tag : str
                New tag name
            tr : float or int
                Tr for the tag
            comment : str
                Comment to add to the new tag
            override : boolean
                Whether to override an existing tag
        """
        tr = tr or time.time()
        if override:
            c = self.execute("""
                insert into %t_tag(__tr, __name, __comment)
                    values(%s, %s, %s)
                on conflict(__name)
                do update
                    set __tr = %s, __comment = %s
            """, (tr, tag, comment, tr, comment))
        else:
            c = self.execute("""
                insert into %t_tag(__tr, __name, __comment)
                    values(%s, %s, %s)
            """, (tr, tag, comment))
        c.execute("commit")

    def copyTag(self, tag, new_tag, comment="", override=False):
        """Creates new tag with the same Tr as an existing tag
        
        Parameters
        ----------
            tag : str
                Exisitng tag name
            new_tag : str
                New tag name
            comment : str
                Comment to add to the new tag
            override : boolean
                If true and ``new_tag`` already exists, it will be moved to the Tr corresponding to ``tag``
        """
        c = self.execute("select __tr from %t_tag where __name=%s", (tag,))
        tup = c.fetchone()
        tr = None
        if tup: tr = tup[0]
        if tr is not None:
            self.tag(new_tag, comment=comment, override=override, tr=tr)
        return tr
