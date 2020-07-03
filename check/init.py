from ext.mysqlsh_plugins_common import register_plugin
from ext.check import trx
from ext.check import queries
from ext.check import locks
from ext.check import schema
from ext.check import other


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
           "check"
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
           "check"
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
           "check"
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
           "check"
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
           "check"
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
                },{
                   "name": "session",
                   "brief": "The session to be used on the operation.",
                   "type": "object",
                   "classes": ["Session", "ClassicSession"],
                   "required": False
                }
            ]
           },
           "check"
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
           "check"
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
           "check"
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
           "check"
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
           "check"
     )  


register_plugin("getLocks", locks.show_locks,
           {"brief": "Prints the locks held by threads",
             "parameters": [{
                   "name": "limit",
                   "brief": "The amount of query to return. Default is 10.",
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
           "check"
     )   

register_plugin("getRunningStatements", trx.get_statements_running,
           {"brief": "Prints the statements in one running transaction identified by thread ID",
             "parameters": [{
                   "name": "limit",
                   "brief": "The amount of running thread to return. Default is 10.",
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
           "check"
     )          

register_plugin("getNonInnoDBTables", schema.get_noninnodb_tables,
           {"brief": "Prints all tables not using InnoDB Storage Engine",
             "parameters": [{
                   "name": "session",
                   "brief": "The session to be used on the operation.",
                   "type": "object",
                   "classes": ["Session", "ClassicSession"],
                   "required": False
                }
            ]
           },
           "check"
     )   

register_plugin("getInnoDBTablesWithNoPK", schema.get_innodb_with_nopk,
           {"brief": "Prints all InnoDB tables not habing a Primary Key or a non NULL unique key",
             "parameters": [{
                   "name": "session",
                   "brief": "The session to be used on the operation.",
                   "type": "object",
                   "classes": ["Session", "ClassicSession"],
                   "required": False
                }
            ]
           },
           "check"
     )

register_plugin("getCascadingFK", schema.get_cascading_fk,
           {"brief": "Prints all foreign keys with cascading constraints",
             "parameters": [{
                   "name": "session",
                   "brief": "The session to be used on the operation.",
                   "type": "object",
                   "classes": ["Session", "ClassicSession"],
                   "required": False
                }
            ]
           },
           "check"
     ) 

register_plugin("getAmountDDL", other.get_amount_ddl,
           {"brief": "Prints a summary of the amount of DDL statements performed since server start",
             "parameters": [{
                   "name": "session",
                   "brief": "The session to be used on the operation.",
                   "type": "object",
                   "classes": ["Session", "ClassicSession"],
                   "required": False
                }
            ]
           },
           "check"
     )       