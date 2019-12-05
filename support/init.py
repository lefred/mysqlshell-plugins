# init.py
# -------


from ext.mysqlsh_plugins_common import register_plugin
from ext.support import fetch

register_plugin("fetchInfo", fetch.get_fetch_info,
           {"brief": "Fetch info from the system",
             "parameters": [{
                "name": "mysql",
                "brief": "Fetch info from MySQL (enabled by default)",
                "type": "bool",
                "required": False
             },{
                "name": "os",
                "brief": "Fetch info from Operating System (requires to have the Shell running locally)"
                         "This is disabled by default",
                "type": "bool",
                "required": False
             },{
                "name": "advices",
                "brief": "Print eventual advices",
                "type": "bool",
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
           "support",
           {
             "brief": "Getting Information useful for requesting help.",
             "details": [
                 "A collection of methods useful when requesting help such as support "
                 "or Community Slack and Forums"
             ]
           }
     )