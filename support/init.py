from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class support:
    """
    Getting Information useful for requesting help.

    A collection of methods useful when requesting help such as support
    or Community Slack and Forums
    """

from support import fetch
