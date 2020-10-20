# collations/init.py
# --------------
# Initializes the collations plugins.

from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class collations:
    """
    Collation utilities

    A collection of collation utilites.
    """

from collations import check
from collations import outoforder
