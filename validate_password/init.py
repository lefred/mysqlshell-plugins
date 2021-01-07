#    Plugin for PASSWORD VALIDATION Management
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
class validate_password:
    """
    MySQL PASSWORD VALIDATION management and utilities.
    
    A collection of functions to:

        Install PASSWORD VALIDATION plugin on a MySQL database, SOURCE-REPLICA, AND MySQL InnoDB Cluster / Group Replication databases Automatically USING SINGLE API COMMAND

        Set PASSWORD VALIDATION  Policy on a MySQL database, SOURCE-REPLICA, AND MySQL InnoDB Cluster / Group Replication databases Automatically USING SINGLE API COMMAND
        
        On a SOURCE-REPLICA environment, the PASSWORD VALIDATION plugin has to be installed on both, separately

            1, On SOURCE

                mysqlsh > validate_password.installPlugin()

            2. On REPLICA

                mysqlsh > validate_password.installPlugin()

        On a SOURCE-REPLICA environment, set PASSWORD VALIDATION policy has to be executed on both, separately

            1. On SOURCE

                mysqlsh > validate_password.setPolicy()

            2. On REPLICA

                mysqlsh > validate_password.setPolicy()

        On a multi-cluster environment (InnoDB Cluster replication to Group Replication), the PASSWORD VALIDATION plugin has to be installed on both, separately.

            1. On PRIMARY node of InnoDB Cluster as Cluster SOURCE

                mysqlsh > validate_password.installPlugin()

            2. On PRIMARY node of Group Replication as Cluster REPLICA

                mysqlsh > validate_password.installPlugin()

        On a multi-cluster environment (InnoDB Cluster replication to Group Replication, set PASSWORD VALIDATION policy has to be executed on both, separately.

            1. On PRIMARY node of InnoDB Cluster as Cluster SOURCE

                mysqlsh > validate_password.setPolicy()

            2. On PRIMARY node of Group Replication as Cluster REPLICA

                mysqlsh > validate_password.setPolicy()
    """

from validate_password import password_validation
