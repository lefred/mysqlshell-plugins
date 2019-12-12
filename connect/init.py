# init.py
# -------


from ext.mysqlsh_plugins_common import register_plugin
from ext.connect import mycnf

register_plugin("mycnf", mycnf.connect_with_mycnf,
           {"brief": "Connect to MySQL using old .my.cnf file",
             "parameters": [{
                "name": "mysqlx",
                "brief": "is the connection expected to use MySQL X Protocol ?",
                "type": "bool",
                "required": False
             },{
                "name": "file",
                "brief": "specific my.cnf file locatation, default is ~/.my.cnf",
                "type": "string",
                "required": False
             }
            ]
           },
           "connect",
           {
             "brief": "Connect to MySQL",
             "details": [
                 "Plugin to connect using the old my.cnf file"
             ]
           }
     )