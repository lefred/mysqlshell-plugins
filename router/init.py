# init.py
# -------

#from router import status as router_status
from router.myrouter import MyRouter

from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class router:
    """
    MySQL Router Object.

    MySQL Router Object.
    """

@plugin_function("router.create")
def create(uri):
    """
    Create the MySQL Router Object.

    Args:
        uri (string): Connection uri to Router's HTTP interface.

    Returns:
        The newly created Router object
    """
    my_router = MyRouter(uri)
    return {
         'connections': lambda route_to_find="": my_router.connections(route_to_find),
         'status': lambda: my_router.status(),
         'api': my_router.api
    }


@plugin_function("router.createRestUser")
def createRestUser(session=None):
    """
    Create the MySQL Router REST API user in MySQL MetaData.

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    # check if we are connected to a server with metadat table
    stmt = """SELECT major FROM mysql_innodb_cluster_metadata.schema_version"""
    result = session.run_sql(stmt)
    if result:
       if result.fetch_one()[0] < 2:
           print("ERROR: this is not a valid MySQL Server, the mysql_innodb_cluster_metada is to old!")
           return
    else:
           print("ERROR: this is not a valid MySQL Server, no mysql_innodb_cluster_metada found!")
           return
    #get the current user connected
    username = shell.parse_uri(session.uri)['user']
    stmt = """REPLACE INTO mysql_innodb_cluster_metadata.router_rest_accounts VALUES
              ((SELECT cluster_id FROM mysql_innodb_cluster_metadata.v2_clusters LIMIT 1), "{}", "modular_crypt_format",
                 (SELECT authentication_string from mysql.user WHERE user="{}"), NULL, NULL, NULL);""".format(username, username)
    result = session.run_sql(stmt)
    if result:
        print("You can now use '{}' to authenticate to MySQL Router's REST API.".format(username))
        print("Use myrouter=router.create() to create an object to monitor.")