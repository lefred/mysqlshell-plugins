# init.py
# -------
from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class demo:
    """
    A demo plugin that showcases the shell's plugin feature.

    This plugin is used only for demo purpose.
    """

from demo import oracle8ball

@plugin_function("demo.helloWorld")
def hello_world():
    """
    Simple function that prints "Hello world!"

    Just say Hello World

    Returns:
        Nothing
    """
    print("Hello world!")

@plugin_function("demo.showSchemas")
def show_schemas(session=None):
    """
    Lists all database schemas

    Sample function that works either with a session passed as parameter or
    with the global session of the MySQL Shell.

    Args:
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
        r = session.run_sql("show schemas")
        shell.dump_rows(r)
