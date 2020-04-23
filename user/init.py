from ext.mysqlsh_plugins_common import register_plugin
from ext.user import create


register_plugin("create", create.create_user,
           {"brief": "Wizard to create a user",
             "parameters": [{
                "name": "session",
                "brief": "The session to be used on the operation.",
                "type": "object",
                "classes": ["Session", "ClassicSession"],
                "required": False
            }
            ]
           },
           "user",
           {
             "brief": "Junior DBA Wizard to manage users.",
             "details": [
                 "A collection of wizards to manage users for junior DBAs"
             ]
           }
     )