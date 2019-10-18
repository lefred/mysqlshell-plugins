from ext.mysqlsh_plugins_common import register_plugin
from ext.maintenance import shutdown


register_plugin("shutdown", shutdown.shutdown,
           {"brief": "Stop the instance using Std Protocol",
             "parameters": [{
                "name": "session",
                "brief": "The session to be used on the operation.",
                "type": "object",
                "classes": ["Session", "ClassicSession"],
                "required": False
            }
            ]
           },
           "maintenance",
           {
             "brief": "Server management and utilities.",
             "details": [
                 "A collection of Server management tools and utilities"
             ]
           }
     )
