from ext.mysqlsh_plugins_common import register_plugin
from ext.audit import trx


register_plugin("showTrxSize", trx.show_trx_size,
           {"brief": "Prints Transactions Size from last binlog",
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
