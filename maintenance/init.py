from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class maintenance:
    """
    Server management and utilities.

    A collection of MySQL Server management tools and utilities.
    """

from maintenance import shutdown
