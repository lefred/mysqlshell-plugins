# schema/init.py
# --------------
# Initializes the schema_utils plugin.

# Contents
# --------
# This plugin will define the following functions
#  - deleteProcedures([schema][, routine][, session])
#  - showDefaults(table[, schema][, session])
#  - showInvalidDates([table][, schema][, session])
#  - showProcedures([schema][, session])
#  - showRoutines([schema][, session])

# Usage example:
# --------------
#
#  MySQL  > 2019-06-26 18:07:50 >
# JS> schema_utils.showDefaults('testing')
# No session specified. Either pass a session object to this function or connect the shell to a database
#  MySQL  > 2019-06-26 18:07:52 >
# JS> \c root@localhost
# Creating a session to 'root@localhost'
# Fetching schema names for autocompletion... Press ^C to stop.
# Your MySQL connection id is 15 (X protocol)
# Server version: 8.0.16 MySQL Community Server - GPL
# No default schema selected; type \use <schema> to set one.
# MySQL 8.0.16 > > localhost:33060+ > > 2019-06-26 18:08:01 >
# JS> schema_utils.showDefaults('testing')
# No schema specified. Either pass a schema name or use one
# MySQL 8.0.16 > > localhost:33060+ > > 2019-06-26 18:08:04 >
# JS> schema_utils.showDefaults('testing', 'big')
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | ColumnName                     | Type            | Default                                            | Example                   |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | id                             | int             | None                                               | NULL                      |
# | varchar_val                    | varchar         | None                                               | NULL                      |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# Total: 2
#
# MySQL 8.0.16 > > localhost:33060+ > > > test > 2019-06-26 18:11:01 >
# JS> schema_utils.showDefaults('default_test')
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | ColumnName                     | Type            | Default                                            | Example                   |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | bi_col_exp                     | bigint          | (8 * 8)                                            | 64                        |
# | d_col                          | date            | curdate()                                          | 2019-06-26                |
# | d_col_exp                      | date            | (curdate() + 8)                                    | 20190634                  |
# | dt_col                         | datetime        | CURRENT_TIMESTAMP                                  | 2019-06-26 18:11:16       |
# | vc_col_exp                     | varchar         | concat(_utf8mb4\'test\',_utf8mb4\'test\')          | testtest                  |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# Total: 5
#

from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class schema_utils:
    """
    Schema management and utilities.

    A collection of schema management tools and related
    utilities that work on schemas.
    """

from schema import utils
from schema import procedures
from schema import defaults
from schema import dates
from schema import routines
from schema import csv
