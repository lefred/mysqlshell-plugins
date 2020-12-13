#    Group Replication MySQL Shell Plugin
#    A community version of operator for Group Replication
#
#    Copyright (C) 2020  Hananto Wicaksono, hananto.wicaksono@gmail.com
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.


from mysqlsh.plugin_manager import plugin, plugin_function

@plugin
class group_replication:
    """
    MySQL Group Replication management and utilities.
    A collection of functions to handle MySQL Group Replication
    without using MySQL InnoDB Cluster (no metadata)
    """

from group_replication import gr

