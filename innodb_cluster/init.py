from mysqlsh.plugin_manager import plugin, plugin_function


@plugin
class innodb_cluster:
    """
     MySQL InnoDB Cluster management and utilities.

     A collection of MySQL InnoDB Cluster management tools and related
     utilities.
    """

from innodb_cluster import secondary
from innodb_cluster import logs
