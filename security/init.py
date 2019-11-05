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


from ext.mysqlsh_plugins_common import register_plugin
from ext.security import expire as security_expire

register_plugin("showPasswordExpire", security_expire.show_password_expire,
                {
                    "brief": "Lists all accounts and their expiration date",
                    "parameters": [
                        {
                            "name": "show_expire",
                            "brief": "List expired passwords too",
                            "type": "bool",
                            "required": False
                        },
                        {
                            "name": "session",
                            "brief": "The session to be used on the operation.",
                            "type": "object",
                            "classes": ["Session", "ClassicSession"],
                            "required": False
                        }
                    ]
                },
                "security",
                {
                    "brief": "Security management and utilities.",
                    "details": [
                        "A collection of security management tools and related "
                        "utilities"
                    ]
                })

register_plugin("showPasswordExpireSoon", security_expire.show_password_expire_soon,
                {
                    "brief": "Lists all accounts that will expire in specific days",
                    "parameters": [
                        {
                            "name": "expire_in_days",
                            "brief": "List accounts that will expire in that range upper limit, if none provided, the default is 30",
                            "type": "bool",
                            "required": False
                        },
                        {
                            "name": "session",
                            "brief": "The session to be used on the operation.",
                            "type": "object",
                            "classes": ["Session", "ClassicSession"],
                            "required": False
                        }
                    ]
                },
                "security"
                )