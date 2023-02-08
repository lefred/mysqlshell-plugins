# init.py
# -------
import mysqlsh
from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class branching:
    """
    A branching plugin that adds branching functionality to the shell.
    """

@plugin_function("branching.createBranch")
def create_branch(schema_name, branch_name, session=None):
    """
    Creates a new branch of the provided database.

    Args:
        schema_name (string): The name of the database to be branched
        branch_name (string): The name of the branch to be created
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    Returns:
        Nothing
    """
    if session is None:
        shell = mysqlsh.globals.shell
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                "function or connect the shell to a database")
            return
    if session is not None:
        # Create the new database name
        branched_schema_name = schema_name + '$$' + branch_name

        # Create the new database
        session.run_sql(f"CREATE DATABASE `{branched_schema_name}`")

        # Get a list of tables in the original database
        tables = session.run_sql(f"SHOW TABLES IN `{schema_name}`").fetch_all()

        # Iterate through the tables and copy them to the new database
        for table in tables:
            # Get the table name
            table_name = table[0]
            # Copy the table`
            session.run_sql(f"CREATE TABLE `{branched_schema_name}`.{table_name} LIKE `{schema_name}`.{table_name}")
            session.run_sql(f"INSERT INTO `{branched_schema_name}`.{table_name} SELECT * FROM `{schema_name}`.{table_name}")

        # Get a list of views in the original database
        views = session.run_sql(f"SHOW FULL TABLES IN `{schema_name}` WHERE Table_Type='View'").fetch_all()

        # Iterate through the views and copy them to the new database
        for view in views:
            # Get the view name
            view_name = view[0]
            session.run_sql(f"CREATE VIEW `{branched_schema_name}`.{view_name} AS SELECT * FROM `{schema_name}`.{view_name}")

@plugin_function("branching.deleteBranch")
def delete_branch(schema_name, branch_name, session=None):
    """
    Delete a branch of the provided database.

    Args:
        schema_name (string): The name of the database to delete the branch from
        branch_name (string): The name of the branch to be created
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    Returns:
        Nothing
    """
    if session is None:
        shell = mysqlsh.globals.shell
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                "function or connect the shell to a database")
            return
    if session is not None:
        session.run_sql(f"DROP DATABASE IF EXISTS `{schema_name}$${branch_name}`")

@plugin_function("branching.listBranches")
def list_branches(schema_name, session=None):
    """
    List the branches of the provided database.

    Args:
        schema_name (string): The name of the database to list the branches from
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    Returns:
        Nothing
    """
    if session is None:
        shell = mysqlsh.globals.shell
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                "function or connect the shell to a database")
            return
    if session is not None:
        databases = session.run_sql(f"SHOW DATABASES").fetch_all()

        # Iterate through the databases, split on $$ and if the first part matches
        # the schema name, print the branch name
        for database in databases:
            database_name_parts = database[0].split('$$')
            database_name = database_name_parts[0]
            database_branch = database_name_parts[1] if len(database_name_parts) > 1 else "mainline"
            if database_name == schema_name:
                print(database_branch)

