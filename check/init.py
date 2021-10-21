from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class check:
    """
    Check management and utilities.

    A collection of tools and utilities to perform checks on your
    MySQL Database Server.
    """

from check import workld
from check import trx
from check import queries
from check import schema
from check import other
from check import gtid
