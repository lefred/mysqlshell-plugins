from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show

@plugin
class logs:
    """
    MySQL Logs Utility.

    A collection of tools to manage and get information
    related to the logs of MySQL Database Server
    """

from logs import error