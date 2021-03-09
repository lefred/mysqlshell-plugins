# init.py
# -------

from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class innodb:
   """
    InnoDB management and utilities.

    A collection of InnoDB management tools and related
    utilities that work on InnoDB Engine.
   """

from innodb import fragmented
from innodb import progress
from innodb import bufferpool
from innodb import autoinc
from innodb import checkpoint
