ConDB REST API
==============


Getting data - <URL prefix>/get
-------------------------------

HTTP method: GET

Arguments
~~~~~~~~~

* folder: text, required - folder name. If the folder is in non-default namespace (schema), the folder name is in the format: <namespace>.<folder name>.
* t: int ot float - Validity timestamp to get data for.
* t0, t1: int or float - Valitity time range to get data for. If t is specified, t0 and t1 are ignored.
* data_type: text - data type to get data for. If unspecified, the data will be returned regardless of the data types.
* format: text, optional - format of the output representation, ``csv`` or ``json``. Default: "csv"

Example:

    .. code-block:: shell

        $ curl "http://host:8080/path/get?folder=my_schema.my_data&t=125436.0&format=json"

Adding data - <URL prefix>/put
------------------------------

HTTP method: POST
Authentication required

Arguments
~~~~~~~~~

* folder: text, required - folder name. If the folder is in non-default namespace (schema), the folder name is in the format: <namespace>.<folder name>.
* tr: float, int or text - the "record time" to associate the new data with. Default: current time.
* data_type: text - data type to associate the data with. Default - blank, "".

Request body must contain CSV-formatted data with first line containing list of columns in the CSV file. First 2 names must be "channel" and "tv".
The rest must be a list of one or more data columns.

Example:

    .. code-block::
    
        channel,tv,x1,x2,y
        1,137,25.4,37.3,-3
        2,137,26.4,137.3,-2
        2,140,27.4,237.5,-2
        3,134,28.4,137.7,-1

The ``put`` method requires authentication. Currently, the only authentication implemented mechanism is ``digest authentication`` described in :rfc:`2617`.
This authentication mechanism is supported by `curl <https://curl.se/curl>`_ and by `requests <https://docs.python-requests.org/en/latest/index.html>`_ Python library. To use is with ``curl``:

    .. code-block:: shell
    
        $ curl -X POST -T data.csv --digest -u username:password http://.../put?folder=...
        
To use with ``requests`` library:

    .. code-block:: python

        import requests
        from requests.auth import HTTPDigestAuth

        response = requests.post(url, data, auth=HTTPDigestAuth(username, password))


Tagging database state - <URL prefix>/tag
-----------------------------------------

HTTP method: POST
Authentication required

Arguments
~~~~~~~~~

* folder: text, required - folder name. If the folder is in non-default namespace (schema), the folder name is in the format: <namespace>.<folder name>.
* tr: float, int or text, optional - the "record time" to associate the new data with. Default: current time.
* copy_from: text, optional - existing tag to make a copy. If ``copy_from`` is used, ``tr`` is ignored.
* override: text, "yes" or "no", optional - if "yes" and the tag with the same name exists, the tag will be moved to the new Tr. Default is "no"

The method requires ``digest authentication``, same as the ``put`` method.

Listing existing tags - <URL prefix>/tags
-----------------------------------------

HTTP method: GET

Arguments
~~~~~~~~~

* folder: text, required - folder name. If the folder is in non-default namespace (schema), the folder name is in the format: <namespace>.<folder name>.
* format: text, optional - format of the output representation, ``csv`` or ``json``. Default: "csv"

Listing existing data types - <URL prefix>/data_types
-----------------------------------------------------

HTTP method: GET

Arguments
~~~~~~~~~

* folder: text, required - folder name. If the folder is in non-default namespace (schema), the folder name is in the format: <namespace>.<folder name>.
* format: text, optional - format of the output representation, ``csv`` or ``json``. Default: "csv"
