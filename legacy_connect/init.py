# init.py
# -------

from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class legacy_connect:
    """
    Connect to MySQL like old days.

    Plugin to connect to MySQL using the old my.cnf file.
    """

from legacy_connect import mycnf
