from ext.mysqlsh_plugins_common import register_plugin
from ext.innodb_cluster import secondary


register_plugin("showGroupReplicationSpeed", secondary.show_speed,
           {"brief": "Prints replication information speed information",
             "parameters": [{
                "name": "limit",
                "brief": "For how many seconds the output show be displayed, refreshed every second and default is 10",
                "type": "integer",
                "required": False
              },{
                "name": "session",
                "brief": "The session to be used on the operation. This must be a session to a InnoB Cluster member",
                "type": "object",
                "classes": ["Session", "ClassicSession"],
                "required": False
            }
            ]
           },
           "innodb_cluster",
           {
             "brief": "MySQL InnoDB Cluster management and utilities.",
             "details": [
                 "A collection of MySQL InnoDB Cluster management tools and utilities"
             ]
           }
     )

