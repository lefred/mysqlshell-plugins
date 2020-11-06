# mysql-shell-ex
A collection of MySQL Shell example plugins (MySQL Shell >= 8.0.22).

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
## Missing Modules

It might be possible that for some plugins, your are missing some modules. Usually it is ``python3-requests``.

You can then just install it. However, on some systems, the version of Python 3 doesn't match the version
linked with MySQL Shell. This is the case with OL7/OL8 where Python 3.6 is installed and MySQL Shell provides
Python 3.8.

If this is the case for you, you can install the missing modules within MySQL Shell too using ``mysqlsh --pum pip``.

**Example with requests**:

For the router and proxySQL plugin, ``requests`` is required, if you don't have it installed, when you start MySQL Shell, you
will get this message:

```
Error importing module 'requests', check if it's installed (Python 3.7.7)
```

You have 2 options, install it system wide (available for all users), if you have root access, or just for your user:

```
$ sudo mysqlsh --pym pip install requests
```

or 

```
$ mysqlsh --pym pip install --user requests
```

That's it ! 


## Extending the MySQL Shell

Please take a look at the demo/init.py file to learn how to extend the MySQL Shell with plugins.

