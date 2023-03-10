from webpie import WPApp, WPHandler, Response, sanitize
from wsdbtools import ConnectionPool, DbDig
from condb import ConDB, signature, __version__ as condb_version
import time, sys, hashlib, os, random, traceback, yaml, json
from datetime import datetime, timedelta, tzinfo
from timelib import text2timestamp, epoch
from threading import RLock, Lock, Condition
import threading, re
from rfc2617 import digest_server

def dtfmt(x, fmt):
    return x.strftime(fmt) if x else ''
    
def as_number(x):
    if not x:   return ''
    e = epoch(x)
    if e == int(e): e = int(e)
    return e
    
def as_json(x):
    return '"Date(%s,%s,%s,%s,%s,%s)"' % (
        x.year, x.month-1, x.day, x.hour, x.minute, x.second)
        
def format_tv(x, numeric):
    if numeric: return as_number(x)
    else:   return dtfmt(x, "%Y-%m-%d&nbsp;%H:%M:%S&nbsp;%Z")
        
def nones_to_nulls(x):
    if type(x) == type([]) or type(x) == type(()):
        return [z if z != None else "null" for z in x ]
    elif x == None: return "null"
    else:   return x

           
class ServerApp(WPApp):

    def __init__(self, rootclass, config):
        WPApp.__init__(self, rootclass)
        self.Config = config
        server_cfg = self.Config.get('Server', {})
        gui_cfg = self.Config.get('GUI', {})
        self.ServerPassword = server_cfg.get('password')
        self.Title = gui_cfg.get('title', "Conditions Database")
        self.CacheTTL = int(server_cfg.get('cache_ttl', 3600))
        self.TaggedCacheTTL = int(server_cfg.get('tagged_cache_ttl', 3600*24*7))

        self.PutPasswords = server_cfg.get("passwords", {})     # {folder: {user:password}}
       
        #
        # Init DB connection pool 
        #
        db_cfg = self.Config.get('Database', {})
        self.DBName = db_cfg['name']
        self.User = db_cfg['user']
        self.Host = db_cfg["host"]
        self.Port = int(db_cfg["port"])
        self.Password = db_cfg.get('password')
            
        self.Namespace = db_cfg.get("namespace", 'public')
            
        connstr = "dbname=%s user=%s password=%s host=%s port=%s" % \
            (self.DBName, self.User, self.Password, self.Host, self.Port)

        self.ConnPool = ConnectionPool(postgres=connstr, idle_timeout=5)
        
        self.initJinjaEnvironment(
            tempdirs=[os.environ["JINJA_TEMPLATES_LOCATION"]],
            filters = {
                "format_tv":    format_tv,
                "dtfmt":    dtfmt,
                "as_number":    as_number,
                "nones_to_nulls":    nones_to_nulls,
                "as_json":    as_json
            },
            globals = 
            {    "GLOBAL_Title":         self.Title,
                 "GLOBAL_GUI_Version":   None, 
                 "GLOBAL_API_Version":   condb_version  
            }
        )
        
    def getPassword(self, folder, user):
        return self.PutPasswords.get(folder, {}).get(user) or self.PutPasswords.get("*", {}).get(user)

    def db(self):
        conn = self.ConnPool.connect()
        #print("App.db(): connection:", id(conn), conn)
        return ConDB(connection = conn)


class Handler(WPHandler):
    def __init__(self, req, app):
        WPHandler.__init__(self, req, app)
        #self.B = DataBrowser(req, app)
        
    def probe(self, req, relpath, **args):
        try:    
            c = self.App.db().cursor()
            c.execute("select 1")
            tup = c.fetchone()
            if tup[0] == 1:
                return Response("OK")
            else:
                raise ValueError("Data mismatch. Expected (1,), got %s" % (tup,))
        except:
            return "Probe error: %s" % (traceback.format_exc(),), 500

    def version(self, req, relpath, **args):
        return '{ "GUI":"%s", "API":"%s", "Version":"%s_%s" }\n' % \
                    (GUI_Version, API_Version, GUI_Version, API_Version) \
                ,"text/json"
    
    def dataTupleToCSV(self, tup):
        text_values = []
        for v in tup:
            if isinstance(v, str):
                if ',' in v or '\n' in v:
                    v = '"' + v.replace('"', '""') + '"'
            elif isinstance(v, list):
                    # assume list of numerics
                    v = '"' + str(v) + '"'
            else:
                    v = str(v)
            text_values.append(v)
        return ','.join(text_values)
        
    def csv_iterator_from_iter(self, data, data_columns, include_tr=False, include_data_type=False):
        headline = "channel,tv,"
        if include_tr:  headline += "tr,"
        if include_data_type:  headline += "data_type,"
        
        yield headline+','.join(data_columns)+'\n'
        for tup in data:
            if not include_data_type:   tup = tup[:3] + tup[4:]
            if not include_tr:          tup = tup[:2] + tup[3:]
            vtxt = self.dataTupleToCSV(tup)
            yield vtxt + "\n"
            
    def json_iterator_from_iter(self, data, data_columns, include_tr=False, include_data_type=False):
        yield "[\n"
        first_line = True
        for tup in data:
            out = '  '
            if not first_line:
                out = ',\n  '
            row = {
                "channel": tup[0],
                "tv": tup[1]
            }
            if include_tr:  row["tr"] = tup[2]
            if include_data_type:  row["data_type"] = tup[3]
            for column_name, value in zip(data_columns, tup[4:]):
                row[column_name] = value
            out += json.dumps(row)
            yield out
            first_line = False
        yield "\n]\n"

    def mergeLines(self, iter, maxlen=10000):
        buf = []
        total = 0
        for l in iter:
            n = len(l)
            if n + total > maxlen:
                yield ''.join(buf)
                buf = []
                total = 0
            buf.append(l)
            total += n
        if buf:
            yield ''.join(buf)

    def data_output_generator(self, data, data_columns, include_tr=False, include_data_type=False, format="csv"):
        formatted = self.csv_iterator_from_iter(data, data_columns, include_tr=include_tr, include_data_type=include_data_type) if format == "csv" \
                else self.json_iterator_from_iter(data, data_columns, include_tr=include_tr, include_data_type=include_data_type)
        return self.mergeLines(formatted)

    def sortTuples(self, data, sort_spec):
        # sorts data in place !
        # sort_spec: col1,col2
        #        or: col1
        # column can be either "tv" or "channel"
        #
        # data is assumed to be [(channel, tv, ...), ...]
        #
        
        if sort_spec == 'tv':
            data.sort(lambda x, y:  cmp(x[1], y[1]))
        elif sort_spec == 'channel':
            data.sort(lambda x, y:  cmp(x[0], y[0]))
        elif sort_spec == 'channel,tv':
            data.sort(lambda x, y:  cmp(x[0], y[0]) or cmp(x[1], y[1]))
        elif sort_spec == 'tv,channel':
            data.sort(lambda x, y:  cmp(x[1], y[1]) or cmp(x[0], y[0]))
        return data        

    def filter_channels(self, data, channel_ranges):
        if not channel_ranges or channel_ranges == [(None, None)]:
            yield from data
        else:
            for tup in data:
                channel = tup[0]
                for c0, c1 in channel_ranges:
                    if (c0 is None or channel >= c0) and (c1 is None or channel <= c1):
                        yield tup

    @sanitize()
    def get(self, req, relpath, folder=None, t=None, t0=None, t1=None, include_tr="no", include_data_type=None,
                tr=None, format="csv", data_type=None, **args):
        #print "get(%s,%s,%s)" % (folder, t0, t1)
        
        if t0 is not None:  t0 = float(t0)
        if t1 is not None:  t1 = float(t1)

        if t is not None:  
            t0 = t1 = float(t)
            
        if tr is not None:
            tr = text2timestamp(tr)

        include_tr = include_tr == "yes"
        include_data_type = include_data_type == "yes" or (data_type is None and include_data_type != "no")
        
        folder_name = folder
        folder = self.App.db().openFolder(folder)
        if folder is None:
            return Response("Table %s does not exist" % (folder_name,), status=404)

        lines = self.getData(folder, t0, t1, tr=tr, data_type=data_type, **args)

        if lines == None:
            lines = []

        lines = list(lines)
        #print("get: lines:")
        #for l in lines:
        #    print(l)

        lines = self.data_output_generator(lines, folder.DataColumns, include_tr=include_tr, include_data_type=include_data_type,
                    format = format)
        resp = Response(app_iter = lines, content_type=f'text/{format}')
        cache_ttl = self.App.CacheTTL
        if "tag" in args:
            cache_ttl = self.App.TaggedCacheTTL
        cache_ttl = int(random.uniform(cache_ttl * 0.9, cache_ttl * 1.2)+0.5)
        resp.cache_expires(cache_ttl)
        return resp

    def getData(self, folder, t0, t1, 
                    tr = None,
                    channels=None,
                    tag = None, data_type=None):
        
        #print "getData(%s,%s,%s,%s,%s)" % (folder, t, t0, t1, args)
        
        if tr and tag:
            raise ValueError("Can not specify both rtime and tag")

        channel_ranges = []
        cmin, cmax = None, None
        
        if channels:
            for segment in channels.split(","):
                c01 = segment.split("-", 1)
                if len(c01) < 2:
                    c01 = [c01[0], c01[0]]
                c0, c1 = c01
                c0 = c0 or None                 # convert blanks to None
                c1 = c1 or None
                try:    c0 = int(c0)
                except: pass                    # either None or string
                try:    c1 = int(c1)
                except: pass                    # either None or string
                if (c0, c1) != (None, None):
                    channel_ranges.append((c0, c1))
                    if cmin is None:    cmin = c0
                    if cmax is None:    cmax = c1
                    if c0 is not None:  cmin = min(cmin, c0)
                    if c1 is not None:  cmax = max(cmax, c1)

        channel_ranges = channel_ranges or None
        global_range = (cmin, cmax) if (cmin or cmax) else None

        rows = folder.getData(t0, t1, data_type=data_type, tag = tag, tr=tr, channel_range = global_range)

        if channel_ranges:
            rows = self.filter_channels(rows, channel_ranges)

        return rows

    def parseTuple(self, line):
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        out = []
        while line:
            line = line.strip()
            if line[0] == '"':
                # find closing quote
                i = 1
                found = None
                while i < len(line) and found is None:
                    i = line.find('"', i)
                    if i >= 0:
                        if i+1 < len(line) and line[i+1] == '"':
                            i += 2
                        else:
                            found = i
                if found is None:
                    # no closing quote ??
                    raise ValueError("Error parsing CSV line [%s]: can not find closing quote" % (line,))
                word = line[:found+1]           # include opening and closing quotes
                rest = line[found+1:].strip()   # this will be either empty, or something starting with comma
            else:
                i = line.find(',')
                if i >= 0:
                    word = line[:i]
                    rest = line[i:].strip()
                else:
                    word = line
                    rest = ""
            word = word.strip()
            # at this point, word is either quoted, and then repeating double-quotes need to be un-doubled
            # or it is not quoted
            
            value = None
            
            if word[0] == '"':
                assert word[-1] == '"'
                word = word[1:-1].replace('""', '"')
            
            if word:
                if word[0] == '[':
                    word = word.strip()
                    assert word[-1] == ']'
                    # assume this is list of numbers. List of strings is not supported !
                    words = word[1:-1].split(",")
                    lst = []
                    for w in words:
                        w = w.strip()
                        try:    v = int(w)
                        except: 
                            try:    v = float(w)
                            except: 
                                v = w       # text
                        lst.append(v)
                    value = lst
                else:
                    try:    value = int(word)
                    except: 
                        try:    value = float(word)
                        except: value = word    # text
            out.append(value)
            assert not rest or rest[0] == ','
            line = rest[1:]     # skip comma
                
        return tuple(out)   

    def authenticateSignature(self, req, data):
        salt = req.headers['X-Salt']
        sig = req.headers['X-Signature']
        folder = req.GET['folder']
        digest = signature(self.App.ServerPassword, salt, req.query_string, b"")
        return digest == sig
        
    def authenticate_digest(self, req, folder):
        ok, header = digest_server(folder, req.environ, self.App.getPassword)
        if not ok:
            resp = Response("Authorization required", status=401)
            if header:
                resp.headers['WWW-Authenticate'] = header
            return False, resp
        return True, None

    @sanitize()
    def put(self, req, relpath, folder=None, tr=None, data_types=None, **args):        
        folder_name = folder
        if not folder_name:
            return 400, "Folder mush be specified"
            
        if tr is not None:
            tr = timelib.text2timestamp(tr)
        
        if not data_types:
            data_types = [""]
        else:
            data_types = data_types.split(",")

        if req.method != 'POST':
            return 400

        if "X-Signature" in req.headers:
            check = self.authenticateSignature(req, req.body)
            if not check:
                return 403, "Authentication failed"
        else:
            ok, resp = self.authenticate_digest(req, folder)
            if not ok:  return resp         # authentication failrure

        input = req.body.split(b"\n")
        header = input[0].decode("utf-8").strip()
        columns = [x.strip() for x in header.split(',')]
        if len(columns) < 3 or \
                columns[0].lower() != 'channel' or \
                columns[1].lower() != 'tv':
            return 400, "Invalid header line in the CSV input."
                
        columns = columns[2:]
        #print 'columns: ', columns
        folder = self.App.db().openFolder(folder_name)
        if folder is None:
            return 404, "Folder not found"
            
        data = []
        for i in range(1, len(input)):
            line = input[i].strip()
            if not line:    continue

            tup = self.parseTuple(line)
            if len(tup) < 3 or not isinstance(tup[0], int) or not isinstance(tup[1], (int, float)):
                return 400, f"Invalid data in line {i}"
            data.append(tup)
            
        if not data:
            return Response("OK", status=204)

        #print "len(data)=", len(data)

        for t in data_types:
            folder.addData(data, data_type=t, tr=tr, columns=columns)
        return Response("OK")
        
    @sanitize()
    def tag(self, req, relpath, folder=None, tag=None, tr=None, copy_from=None, override='no', **args):

        if "X-Signature" in req.headers:
            check = self.authenticateSignature(req, req.body)
            if not check:
                return 403, "Authentication failed"
        else:
            ok, resp = self.authenticate_digest(req, folder)
            if not ok:  return resp         # authentication failrure

        folder = self.App.db().openFolder(folder)
        if folder is None:
            return 404, "Folder not found"
        
        if copy_from:
            folder.copyTag(copy_from, tag, override = override == 'yes')
        else:
            tr = text2timestamp(tr)
            folder.tag(tag, override = override == 'yes', tr=tr)
        return Response("OK")

    @sanitize()
    def tags(self, req, relpath, folder=None, format="csv"):
        folder = self.App.db().openFolder(folder)
        if folder is None:
            return 404, "Folder not found"
        tags = folder.tags()
        if format == "csv":
            out = ["name,tr"]
            for tup in tags:
                out.append(self.dataTupleToCSV(tup))
            return "\n".join(out) + "\n", "text/csv"
        else:
            out = [{"tag":tag, "tr":tr} for tag, tr in tags]
            return json.dumps(out), "text/json"

    @sanitize()
    def data_types(self, req, relpath, folder=None, format="csv"):
        folder = self.App.db().openFolder(folder)
        if folder is None:
            return 404, "Folder not found"
        types = folder.dataTypes()
        if format == "csv":
            out = ["name"]
            for typ in types:
                out.append(self.dataTupleToCSV((typ,)))
            return "\n".join(out) + "\n", "text/csv"
        else:
            return json.dumps(list(types)), "text/json"

    def index(self, req, relpath, **args):
        return self.render_to_response("index.html")
        

def create_application(config_file = None):
    config_file = config_file or os.environ["CON_DB_CFG"]
    config = yaml.load(open(config_file, "r"), Loader=yaml.SafeLoader)
    return ServerApp(Handler, config)
    
application = None
if "CON_DB_CFG" in os.environ:
    application = create_application()

if __name__ == "__main__":
    import sys, getopt
    opts, args = getopt.getopt(sys.argv[1:], "c:")
    opts = dict(opts)
    config = opts.get("-c")
    if config:
        print("Using config file:", config)
    print("Starting HTTP server at port 8888...") 
    application = create_application(config)
    application.run_server(8888)



