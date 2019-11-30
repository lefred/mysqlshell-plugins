# init.py
# -------


from ext.mysqlsh_plugins_common import register_plugin
from ext.innodb import fragmented
from ext.innodb import progress
from ext.innodb import bufferpool


register_plugin("getFragmentedTables", fragmented.get_fragmented_tables,
           {"brief": "Prints InnoDB fragmented tables",
             "parameters": [{
                "name": "percent",
                "brief": "Amount of free space to be considered as fragmented",
                "type": "integer",
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
           "innodb",
           {
             "brief": "InnoDB management and utilities.",
             "details": [
                 "A collection of InnoDB management tools and related "
                 "utilities that work on InnoDB Engine"
             ]
           }
     )

register_plugin("getFragmentedTablesDisk", fragmented.get_fragmented_tables_disk,
           {"brief": "Prints InnoDB fragmented tables with disk info",
             "parameters": [{
                "name": "percent",
                "brief": "Amount of free space to be considered as fragmented",
                "type": "integer",
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
           "innodb",
           {
             "brief": "InnoDB management and utilities.",
             "details": [
                 "A collection of InnoDB management tools and related "
                 "utilities that work on InnoDB Engine"
             ]
           }
     )

register_plugin("getAlterProgress", progress.get_alter_progress,
           {"brief": "Prints InnoDB Alter progress info",
             "parameters": [{
                "name": "session",
                "brief": "The session to be used on the operation.",
                "type": "object",
                "classes": ["Session", "ClassicSession"],
                "required": False
            }
            ]
           },
           "innodb",
           {
             "brief": "InnoDB management and utilities.",
             "details": [
                 "A collection of InnoDB management tools and related "
                 "utilities that work on InnoDB Engine"
             ]
           }
     )

register_plugin("getTablesInBP", bufferpool.get_tables_in_bp,
           {"brief": "Prints Tables in BP with some statistics",
             "parameters": [{
                "name": "session",
                "brief": "The session to be used on the operation.",
                "type": "object",
                "classes": ["Session", "ClassicSession"],
                "required": False
            }
            ]
           },
           "innodb",
           {
             "brief": "InnoDB management and utilities.",
             "details": [
                 "A collection of InnoDB management tools and related "
                 "utilities that work on InnoDB Engine"
             ]
           }
     )
