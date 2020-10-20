# init.py
# -------

from router import status as router_status
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
