from ext.mysqlsh_plugins_common import register_plugin
from ext.check import trx
from ext.check import queries


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
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
             ]
           }
     )

register_plugin("showTrxSizeSort", trx.show_trx_size_sort,
           {"brief": "Prints Transactions Size from a binlog",
             "parameters": [{
                "name": "limit",
                "brief": "The maximum transactions to show, default is 10",
                "type": "integer",
                "required": False
            },{
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
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
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
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
             ]
           }
     )

register_plugin("getBinlogsIO", trx.show_binlogs_io,
           {"brief": "Prints the IO statistics of binary logs files available on the server",
             "parameters": [{
                "name": "session",
                "brief": "The session to be used on the operation.",
                "type": "object",
                "classes": ["Session", "ClassicSession"],
                "required": False
            }
            ]
           },
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
             ]
           }
     )

register_plugin("getTrxWithMostStatements", trx.get_trx_most_stmt,
           {"brief": "Prints the transaction with the most amount of statements",
             "parameters": [{
                   "name": "limit",
                   "brief": "The amount of query to return. Default is 1.",
                   "type": "integer",
                   "required": False
                },{
                   "name": "schema",
                   "brief": "The name of the schema to look at.",
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
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
             ]
           }
     )

register_plugin("getTrxWithMostRowsAffected", trx.get_trx_most_rows,
           {"brief": "Prints the transaction with the most amount of statements",
             "parameters": [{
                   "name": "limit",
                   "brief": "The amount of query to return. Default is 1.",
                   "type": "integer",
                   "required": False
                },{
                   "name": "schema",
                   "brief": "The name of the schema to look at.",
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
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
             ]
           }
     )
     


register_plugin("getSlowerQuery", queries.get_queries_95_perc,
           {"brief": "Prints the slowest queries. If the limit is one you can also see all the details about the query",
             "parameters": [{
                   "name": "limit",
                   "brief": "The amount of query to return. Default is 1.",
                   "type": "integer",
                   "required": False
                },{
                   "name": "select",
                   "brief": "Returns queries only with SELECT.",
                   "type": "bool",
                   "required": False
                },{
                   "name": "schema",
                   "brief": "The name of the schema to look at.",
                   "type": "string",
                   "required": False
#                },{
#                   "name": "session",
#                   "brief": "The session to be used on the operation.",
#                   "type": "object",
#                   "classes": ["Session", "ClassicSession"],
#                   "required": False
                }
            ]
           },
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
             ]
           }
     )


register_plugin("getQueryTempDisk", queries.get_queries_temp_disk,
           {"brief": "Prints the queries using temporary tables on disk",
             "parameters": [{
                   "name": "limit",
                   "brief": "The amount of query to return. Default is 1.",
                   "type": "integer",
                   "required": False
                },{
                   "name": "schema",
                   "brief": "The name of the schema to look at.",
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
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
             ]
           }
     )


register_plugin("getFullTableScanQuery", queries.get_queries_ft_scan,
           {"brief": "Prints the queries performing full table scans",
             "parameters": [{
                   "name": "limit",
                   "brief": "The amount of query to return. Default is 1.",
                   "type": "integer",
                   "required": False
                },{
                   "name": "select",
                   "brief": "Returns queries only with SELECT.",
                   "type": "bool",
                   "required": False
                },{
                   "name": "schema",
                   "brief": "The name of the schema to look at.",
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
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
             ]
           }
     )

register_plugin("getQueryMostRowAffected", queries.get_queries_most_rows_affected,
           {"brief": "Prints the statements affecting the most rows",
             "parameters": [{
                   "name": "limit",
                   "brief": "The amount of query to return. Default is 1.",
                   "type": "integer",
                   "required": False
                },{
                   "name": "schema",
                   "brief": "The name of the schema to look at.",
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
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
             ]
           }
     )

register_plugin("getQueryUpdatingSamePK", queries.get_queries_updating_same_pk,
           {"brief": "Prints the statements updating mostly the same PK and therefore having to wait more (hotspot)",
             "parameters": [{
                   "name": "limit",
                   "brief": "The amount of query to return. Default is 1.",
                   "type": "integer",
                   "required": False
                },{
                   "name": "schema",
                   "brief": "The name of the schema to look at.",
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
           "check",
           {
             "brief": "Check management and utilities.",
             "details": [
                 "A collection of Check management tools and utilities"
             ]
           }
     )     