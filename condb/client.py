from .http_client import HTTPClient, ConDBError, ServerSideError, PermissionError, BadRequestError, NotFoundError, WebAPIError
from condb.timelib import epoch
import csv, io
from urllib.parse import quote, unquote
from requests.auth import HTTPDigestAuth

class ConDBClient(HTTPClient):
    
    def __init__(self, url, username=None, password=None):
        """Initializes the ConDB client
        
        Parameters
        ----------
            url : str
                Server URL prefix
            username : str
                Optional username
            password : str
                Optional password
        """
        
        HTTPClient.__init__(self, url)
        self.Username = username
        self.Password = password
        
    def get_data(self, folder, t0, t1=None, tag=None, tr=None, data_type=None, include_data_type=False, channels=None):
        """Get data from folder

        Parameters
        ----------
            folder : str
                Folder name
            t0 : int or float
                Tv to get data for
            t1 : int or float 
                Optional, if specified, the data will be returned for the t0...t1 time interval
            tag : str
                Optional, return data from the tagged database state
            tr : datetime or int or float 
                Optional, return data for previous state of the database identified with tr
            data_type : str
                Optional, return sata for the given data source. Default: ""
            include_data_type : boolean
                Optional, whether to include data_type column. Default: False
            channels : list or tuple or int
                If int, return data for single channel      
                If tuple, return data for a range of channels, e.g.: (100, 151)
                If list, return data for list of channels or channel ranges, e.g.: [10, 15, (23, 27)]
        
        Returns
        -------
        tuple
            (columns, iterable)
                columns - list of column names
                iterable - iterable with tuples with values corresponding the column names
        """
        
        
        url = f"get?folder={folder}"
        if t1 is None:
            url += f"&t={t0}"
        else:
            url += f"&t0={t0}&t1={t1}"
        if tr is not None:
            tr = epoch(tr)
            url += f"&tr={tr}"
        if data_type:
            url += f"&data_type={data_type}"
        if channels is not None:
            if isinstance(channels, (int, tuple)):
                channels = [channels]
            ranges = []
            for spec in channels:
                if isinstance(spec, tuple):
                    c0, c1 = spec
                    if c0 is None:  c0 = ''
                    if c1 is None:  c1 = ''
                    ranges.append(f"{c0}-{c1}")
                else:
                    ranges.append(str(spec))
            if ranges:
                url += f"&channels=" + ",".join(ranges)
        if include_data_type:
            url += "&include_data_type=yes"

        data = self.get_text(url)
        csv_buf = io.StringIO(data)
        reader = csv.reader(csv_buf, delimiter = ",", quoting = csv.QUOTE_MINIMAL, lineterminator="\n")
        columns = next(reader)
        out = []
        for row in reader:
            out_row = []
            for x in row:
                try:    x = int(x)
                except:
                    try:    x = float(x)
                    except:
                        pass
                out_row.append(x)
            out.append(tuple(out_row))
        return columns, out

    def put_data(self, folder, data, columns, tr=None, data_type=None):
        """Add data to folder.
        Requires the username and password.
        
        Parameters
        ----------
            folder : str
                Folder name
            data : list
                List of tuples: [(channel, tv, ...), ...]. Order of columns after tv is the same as in ``data_columns``
            columns : list
                Must begin with ["channel", "tv"]
            data_type : str
                Optional data source, default = ""
            tr : datetime or int or float
                Optional Tr to associate the data with. Default = current time
        """
        
        if self.Username is None or self.Password is None:
            raise ValueError("Username and password are required")
        
        if columns[0] != "channel" or columns[1] != "tv":
            raise ValueError("Channel and tv columns must be first and second in the columns list")
        
        csv_buf = io.StringIO(newline="")
        writer = csv.writer(csv_buf, delimiter = ",", quoting = csv.QUOTE_MINIMAL, lineterminator="\n")
        
        writer.writerow(columns)
        for row in data:
            writer.writerow(row)
        
        url = f"put?folder={folder}"
        if tr is not None:
            tr = epoch(tr)
            url += f"&tr={tr}"
        if data_type:
            data_type = quote(data_type)
            url += f"&data_type={data_type}"

        self.post(url, csv_buf.getvalue(), auth=HTTPDigestAuth(self.Username, self.Password))

    def tag_state(self, folder, tag, tr=None, copy_from=None, override=False):
        """Add tag to a database state.
        Requires the username and password.
        
        Parameters
        ----------
            folder : str
                Folder name
            tag : str
                Tag to add
            tr : datetime or int or float
                Tr, optional. Identifies the database state to tag. Default: current state
            copy_from : str
                Tag the same state as an existing tag
            override : boolean
                If the tag already exists, move the tag to the new Tr
        """
        if self.Username is None or self.Password is None:
             raise ValueError("Username and password are required")

        url = f"tag?folder={folder}&tag={tag}"
        if tr:
            tr = epoch(tr)
            url += f"&tr={tr}"
        if copy_from:
            url += f"&copy_from={copy_from}"
        if override:
            url += "&override=yes"
        
        out = self.post_text(url, "", auth=HTTPDigestAuth(self.Username, self.Password))
        return float(out.strip())

        
        
        
        

