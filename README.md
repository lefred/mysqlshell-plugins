# mysql-shell-ex
A collection of MySQL Shell example plugins. 

After installing this plugin collection into the `.mysqlsh/plugins/ext` folder it will register a new MySQL Shell global object called "ext" and load several plugin categories at MySQL Shell startup. 

To get help about the individual plugins type `\? ext`.

```
MySQL JS> \? ext.schema
NAME
      schema - Schema management and utilities.

SYNTAX
      ext.schema

DESCRIPTION
      A collection of schema management tools and related utilities that work
      on schemas

FUNCTIONS
      help([member])
            Provides help about this object and it's members

      showProcedures([schema][, session])
            Lists all stored procedures.

MySQL JS> ext.schema.showProcedures()
+----------------+-------------------------------------+
| ROUTINE_SCHEMA | ROUTINE_NAME                        |
+----------------+-------------------------------------+
| sys            | extract_schema_from_file_name       |
| sys            | extract_table_from_file_name        |
...

MySQL JS> \py
Switching to Python mode...

MySQL PY> ext.schema.show_procedures()
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

**Please note:** This plugin collection needs to be installed into a folder called `ext`.

### macOS / Linux
```
$ mkdir -p ~/.mysqlsh/plugins
$ git clone https://github.com/mzinner/mysql-shell-ex.git ~/.mysqlsh/plugins/ext
```

### Windows
```
$ mkdir %AppData%MySQL\mysqlsh\plugins
$ git clone https://github.com/mzinner/mysql-shell-ex.git %AppData%MySQL\mysqlsh\plugins\ext
```

## Extending the MySQL Shell

Please take a look at the demo/init.py file to learn how to extend the MySQL Shell with plugins.

