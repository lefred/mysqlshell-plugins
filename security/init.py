# security/init.py
# --------------
# Initializes the security plugins.

# Contents
# --------
# This plugin will define the following functions

# Example
# -------

#  MySQL 8.0.16 > > localhost:33060+ > > 2019-06-26 18:48:48 > 
# JS> ext.security.showPasswordExpire()
# +-------------------------------------------------------------------+-----------------------+------------+
# | concat(sys.quote_identifier(user),'@',sys.quote_identifier(host)) | password_last_changed | expires_in |
# +-------------------------------------------------------------------+-----------------------+------------+
# | `demo`@`%`                                                        | 2019-05-21 12:25:58   | expired    |
# | `fred`@`%`                                                        | 2019-02-07 11:09:01   | expired    |
# | `root`@`localhost`                                                | 2019-02-07 11:07:29   | expired    |
# +-------------------------------------------------------------------+-----------------------+------------+
#  MySQL 8.0.16 > > localhost:33060+ > > 2019-06-26 18:48:50 > 
# JS> ext.security.showPasswordExpire(true)
# +-------------------------------------------------------------------+-----------------------+------------+
# | concat(sys.quote_identifier(user),'@',sys.quote_identifier(host)) | password_last_changed | expires_in |
# +-------------------------------------------------------------------+-----------------------+------------+
# | `demo`@`%`                                                        | 2019-05-21 12:25:58   | expired    |
# | `fred`@`%`                                                        | 2019-02-07 11:09:01   | expired    |
# | `root`@`localhost`                                                | 2019-02-07 11:07:29   | expired    |
# +-------------------------------------------------------------------+-----------------------+------------+
#  MySQL 8.0.16 > > localhost:33060+ > > 2019-06-26 18:48:58 > 
# JS> ext.security.showPasswordExpire(false)


from mysqlsh.plugin_manager import plugin, plugin_function
from security import expire 
from security import authmethod 

@plugin
class security:
   """
    Security management and utilities.

    A collection of security management tools and related utilities"
   """
