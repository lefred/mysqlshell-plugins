# mysql-shell-ex
A collection of MySQL Shell example plugins.

After installing this plugin collection into the `.mysqlsh/plugins/` folder, all the plugins will be automatically loaded at MySQL Shell startup.

To get help about the individual plugins type `\? <plugin>`.

```
MySQL JS> \? schema_utils
NAME
      schema_utils - Schema management and utilities.

DESCRIPTION
      A collection of schema management tools and related utilities that work
      on schemas."

FUNCTIONS
      deleteProcedures([schema][, routine][, session])
            Delete stored procedures.

      help([member])
            Provides help about this object and it's members

      showDefaults(table[, schema][, session])
            Lists the default values of each column in a table.

      showInvalidDates([table][, schema][, session])
            Show Invalid Dates

      showProcedures([schema][, session])
            Lists all stored procedures of either all schemas or a given
            schema.

      showRoutines([schema][, session])
            Show Routines.


MySQL JS> schema_utils.showProcedures()
+----------------+-------------------------------------+
| ROUTINE_SCHEMA | ROUTINE_NAME                        |
+----------------+-------------------------------------+
| sys            | extract_schema_from_file_name       |
| sys            | extract_table_from_file_name        |
...

MySQL JS> \py
Switching to Python mode...

MySQL PY> schema_utils.show_procedures()
+----------------+-------------------------------------+
| ROUTINE_SCHEMA | ROUTINE_NAME                        |
+----------------+-------------------------------------+
| sys            | extract_schema_from_file_name       |
| sys            | extract_table_from_file_name        |
...
```

## Installation
To install this demo on your machine type the following commands after you have installed the MySQL Shell and the git command line tools.

This will clone the repository and copy the files to the right path in order to be automatically loaded on MySQL Shell startup.

### macOS / Linux
```
$ mkdir -p ~/.mysqlsh/plugins
$ git clone https://github.com/lefred/mysqlshell-plugins.git ~/.mysqlsh/plugins
```

### Windows
```
$ mkdir %AppData%\MySQL\mysqlsh\plugins
$ git clone https://github.com/lefred/mysqlshell-plugins.git %AppData%\MySQL\mysqlsh\plugins
```

## Extending the MySQL Shell

Please take a look at the demo/init.py file to learn how to extend the MySQL Shell with plugins.

