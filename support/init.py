from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class support:
    """
    Getting Information useful for requesting help.

    A collection of methods useful when requesting help such as support
    or Community Slack and Forums
    """

    all_functions=[]
    all_functions.append("fred")

from support import fetch
from support import collect

