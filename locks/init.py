from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class locks:
    """
    Locks information utilities.

    A collection of methods to deal with MySQL Locks.
    """

from locks import locks
from locks import locks_tree
