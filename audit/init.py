from ext.mysqlsh_plugins_common import register_plugin
from ext.audit import trx


register_plugin("showTrxSize", trx.show_trx_size,
           {"brief": "Prints Transactions Size from a binlog",
             "parameters": [{
                "name": "binlog",
                "brief": "The binlog file to extract transactions from.",
                "type": "string",
                "required": False
            },{
                "name": "session",
                "brief": "The session to be used on the operation.",
                "type": "object",
                "classes": ["Session", "ClassicSession"],
                "required": False
            }
            ]
           },
           "audit",
           {
             "brief": "Audit management and utilities.",
             "details": [
                 "A collection of Audit management tools and utilities"
             ]
           }
     )

register_plugin("getBinlogs", trx.show_binlogs,
           {"brief": "Prints the list of binary logs available on the server",
             "parameters": [{
                "name": "session",
                "brief": "The session to be used on the operation.",
                "type": "object",
                "classes": ["Session", "ClassicSession"],
                "required": False
            }
            ]
           },
           "audit",
           {
             "brief": "Audit management and utilities.",
             "details": [
                 "A collection of Audit management tools and utilities"
             ]
           }
     )
