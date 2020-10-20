# init.py
# -------

from proxysql.proxysql import ProxySQL

from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class proxysql:
    """
    ProxySQL Object.

    ProxySQL Object.
    """

@plugin_function("proxysql.create")
def create(uri):
    """
    Create the ProxySQL Object.

    Args:
        uri (string): Connection uri to ProxySQL's admin interface.

    Returns:
        The newly created ProxySQL object
    """
    my_proxy = ProxySQL(uri)
    return {
         'status': lambda loop=False: my_proxy.get_status(loop),
         'configure': lambda: my_proxy.configure(),
         'hosts': lambda: my_proxy.get_hosts(),
         'version': lambda: my_proxy.get_version(),
         'hostgroups': lambda: my_proxy.get_hostgroups(),
         'getUsers': lambda hostgroup="": my_proxy.get_user_hostgroup(hostgroup),
         'setUser': lambda hostgroup="", user="", password=False: my_proxy.set_user_hostgroup(hostgroup,user,password),
         'importUsers': lambda hostgroup="", user_search="": my_proxy.import_users(hostgroup, user_search),
         'setUserHostgroup': lambda hostgroup="", user_search="": my_proxy.set_host_group(hostgroup, user_search)
    }
