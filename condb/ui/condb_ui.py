from condb.ui.cli import CLI, CLICommand, InvalidOptions, InvalidArguments
from condb import ConDB, ConDBClient
from condb.timelib import text2timestamp
import sys, csv, io, time, os

class ConDBCLI(CLI):
    
    Opts = "c:"
    Usage = """
        $ condb <command> [<options>] ...
        
        Commands are:
            create              - create folder
            read                - read data directly from the database
            write               - write data directly to the database
            get                 - get data from the server
            put                 - send data to the server
    """

def connect_to_db(opts, dbname):
    import psycopg2
    dbcon = []
    if "-h" in opts:        dbcon.append("host=%s" % (opts["-h"],))
    if "-p" in opts:        dbcon.append("port=%s" % (int(opts["-p"]),))
    if "-U" in opts:        dbcon.append("user=%s" % (opts["-U"],))
    if "-w" in opts:        dbcon.append("password=%s" % (opts["-w"],))
    dbcon.append("dbname=%s" % (dbname,))
    return psycopg2.connect(" ".join(dbcon))

def read_csv(input_file):
    reader = csv.reader(sys.stdin, delimiter = ",", quoting = csv.QUOTE_MINIMAL, lineterminator="\n")
    columns = next(reader)
    if tuple(columns[:2]) != ("channel","tv"):
        print("First row must contain list of columns and first two columns must be channel and tv", file=sys.stderr)
        print("Headline from the file:", headline, file=sys.stderr)
        sys.exit(1)
    data = []
    for row in reader:
        if row:
            if len(row) != len(columns):
                print("Wrong number of columns in row:", *row)
            out_row = []
            for x in row:
                try:    x = int(x)
                except:
                    try:    x = float(x)
                    except:
                        pass
                out_row.append(x)
            data.append(tuple(out_row))
    return columns, data

class CreateCommand(CLICommand):
    
    Opts = "h:p:U:w:cso:R:W:"
    MinArgs = 3
    Usage = """[options] <database name> <folder_name> <column>:<type> [...]
    
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
            """
                
    def __call__(self, command, context, opts, args):
        dbname, folder, data_columns = args[0], args[1], args[2:]
        coltypes = [tuple(a.split(':', 1)) for a in data_columns]
        owner = opts.get("-o")
        drop_existing = "-c" in opts
        read_roles = opts.get("-R", "").split(",")
        write_roles = opts.get("-W", "").split(",")
        grants={"r":[r for r in read_roles if r], "w":[r for r in write_roles if r]}
        if "s" in opts:
            sql = CDFolder.createSQL(folder, coltypes, owner, grants, drop_existing)
            print(s)
        else:
            conn = connect_to_db(opts, dbname)
            con_db = ConDB(conn)
            folder = con_db.createFolder(folder, coltypes, owner, grants, drop_existing)
            print("Folder created")

class ReadCommand(CLICommand):
    Opts = "h:p:U:w:t:T:d:c:"
    MinArgs = 2
    Usage = """[options] <database name> <folder_name>
    
        Options:
            -h <host>
            -p <port>
            -U <user>
            -w <password>

            -t <time>                   Tv, numeric or ISO default = now
            -t <time0>-<time1>          Tv range, numeric or ISO
            -T <tag>    
            -d <data_type>              default = blank
            -c <channel>-<channel>      channel range
    """

    def __call__(self, command, context, opts, args):
        if len(args) != 2:
            raise InvalidArguments()

        dbname, folder = args
        t0 = time.time()
        t1 = None
        tv = opts.get("-t")
        try:
            if tv is not None:
                if "-" in tv:
                    t0, t1 = tv.split("-", 1)
                    t0 = text2timestamp(t0)
                    t1 = text2timestamp(t1)
                else:
                    t0 = text2timestamp(tv)
        except:
            raise InvalidOptions(f"Invalid Tv or Tv range specification: {tv}")
    
        tag = opts.get("-T")
        data_type = opts.get("-d")
        channel_range = opts.get("-c")
        c0, c1 = None, None
        cr_tuple = None
        if channel_range:
            try:
                if "-" in channel_range:
                    c0, c1 = channel_range.split("-", 1)
                    c0 = int(c0)
                    c1 = int(c1)
                else:
                    c0 = c1 = int(channel_range)
            except:
                raise InavlidOption(f"Invalid channel range specification: {channel_range}")
            cr_tuple = (c0, c1)
    
        conn = connect_to_db(opts, dbname)
        folder = ConDB(conn).openFolder(folder)
        data = folder.getData(t0, t1=t1, tag=tag, data_type=data_type, channel_range=cr_tuple)
        for row in data:
            print(*row)
    

class WriteCommand(CLICommand):
    Opts = "h:p:U:w:d:"
    MinArgs = 2
    Usage = """[options] <database name> <folder_name> < <CSV file>
    
        Options:
            -h <host>
            -p <port>
            -U <user>
            -w <password>

            -d <data_type>              default = blank
    """

    def __call__(self, command, context, opts, args):
        if len(args) != 2:
            raise InvalidArguments()

        dbname, folder = args
        data_type = opts.get("-d", "")
    
        conn = connect_to_db(opts, dbname)
        folder = ConDB(conn).openFolder(folder)
    
        columns, data = read_csv(sys.stdin)
        folder.addData(data, data_type=data_type, columns=columns[2:])


class PutCommand(CLICommand):
    Opts = "s:U:w:d:"
    MinArgs = 1
    Usage = """[options] <folder_name> < <CSV file>
    
        Options:
            -s <server URL>             CONDB_SERVER_URL envirinment variable can be used too
            -U <username>
            -w <password>
            -d <data type>
    """

    def __call__(self, command, context, opts, args):
        if len(args) != 1:
            raise InvalidArguments()

        folder = args[0]
        data_type = opts.get("-d", "")
        username = opts.get("-U")
        password = opts.get("-w")
        server_url = opts.get("-s") or os.environ.get("CONDB_SERVER_URL")
        if not server_url:
            raise InvalidOptions("CenDB server URL must be specified with -s option or CONDB_SERVER_URL environment variable")
        client = ConDBClient(server_url, username, password)
        columns, data = read_csv(sys.stdin)
        try:
            client.put_data(folder, data, columns, data_type=data_type)
        except Exception as e:
            print(e, file=sys.stderr)
            sys.exit(1)


class TagCommand(CLICommand):
    Opts = "s:U:w:d:r:T:f"
    MinArgs = 2
    Usage = """[options] <folder_name> <tag name>
    
        Options:
            -s <server URL>             CONDB_SERVER_URL envirinment variable can be used too
            -U <username>
            -w <password>
            -r <tr>                     optional Tr, default=now
            -T <existing tag>           existing tag to copy
            -f                          move the tag to new Tr if exists
    """

    def __call__(self, command, context, opts, args):
        if len(args) != 2:
            raise InvalidArguments()

        folder, tag = args
        data_type = opts.get("-d", "")
        username = opts.get("-U")
        password = opts.get("-w")
        server_url = opts.get("-s") or os.environ.get("CONDB_SERVER_URL")
        if not server_url:
            raise InvalidOptions("CenDB server URL must be specified with -s option or CONDB_SERVER_URL environment variable")

        tr = opts.get("-r")
        if tr: tr = text2timestamp(tr)
        force = "-f" in opts
        copy_from = opts.get("-T")
        
        client = ConDBClient(server_url, username, password)
        try:
            tr = client.tag_state(folder, tag, tr=tr, copy_from=copy_from, override=force)
        except Exception as e:
            print(e, file=sys.stderr)
            sys.exit(1)
        print(tr)


class GetCommand(CLICommand):
    Opts = "s:t:T:d:c:"
    MinArgs = 1
    Usage = """[options] <folder_name>
    
        Options:
            -s <server URL>             CONDB_SERVER_URL envirinment variable can be used too
            -t <time>                   Tv, numeric or ISO default = now
            -t <time0>-<time1>          Tv range, numeric or ISO
            -T <tag>    
            -d <data_type>              default = blank
            -c <channel>                single channel
            -c <channel>-<channel>      channel range
    """

    def __call__(self, command, context, opts, args):
        if len(args) != 1:
            raise InvalidArguments()

        t0 = time.time()
        t1 = None
        tv = opts.get("-t")
        try:
            if tv is not None:
                if "-" in tv:
                    t0, t1 = tv.split("-", 1)
                    t0 = text2timestamp(t0)
                    t1 = text2timestamp(t1)
                else:
                    t0 = text2timestamp(tv)
        except Exception as e:
            raise InvalidOptions(f"Invalid Tv or Tv range specification: {tv}: {e}")
    
        tag = opts.get("-T")
        data_type = opts.get("-d")
        channel_range = opts.get("-c")
        channels = None
        if channel_range:
            try:
                if "-" in channel_range:
                    c0, c1 = channel_range.split("-", 1)
                    c0 = int(c0)
                    c1 = int(c1)
                else:
                    c0 = c1 = int(channel_range)
            except:
                raise InavlidOption(f"Invalid channel range specification: {channel_range}")
            channels = [(c0, c1)]

        folder = args[0]
        data_type = opts.get("-d", "")
        username = opts.get("-U")
        password = opts.get("-w")
        server_url = opts.get("-s") or os.environ.get("CONDB_SERVER_URL")
        if not server_url:
            raise InvalidOptions("CenDB server URL must be specified with -s option or CONDB_SERVER_URL environment variable")
        client = ConDBClient(server_url)

        try:
            columns, data = client.get_data(folder, t0, t1, tag=tag, data_type=data_type, channels=channels)
        except Exception as e:
            print(e, file=sys.stderr)
            sys.exit(1)
        
        writer = csv.writer(open(sys.stdout.fileno(), "w", newline=""), delimiter = ",", quoting = csv.QUOTE_MINIMAL, lineterminator="\n")
        writer.writerow(columns)
        for row in data:
            writer.writerow(row)


def main():
    cli = ConDBCLI(
        "create",   CreateCommand(),
        "write",    WriteCommand(),
        "read",     ReadCommand(),
        "put",      PutCommand(),
        "get",      GetCommand(),
        "tag",      TagCommand()
    )
    cli.run(sys.argv, argv0="condb")

if __name__ == "__main__":
    main()    
    
    
    
    
    
    
    
    