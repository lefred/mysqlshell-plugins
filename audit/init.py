from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class audit:
    """
    Audit table management and utilities.

    A collection of tools and utilities to perform audit track on your
    MySQL Database Server.
    """

from audit import methods
