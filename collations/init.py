# collations/init.py
# --------------
# Initializes the collations plugins.

from ext.mysqlsh_plugins_common import register_plugin
from ext.collations import check as check_src 
from ext.collations import outoforder as outoforder_src 


register_plugin("nonUnique", check_src.non_unique,
                {
                    "brief": "Find non-unique values in a column for a given collation",
                    "parameters": [
                        {
                        "name": "table",
                        "brief": "Table name to use.",
                        "type": "string",
                        "required": True},
                        {
                        "name": "column",
                        "brief": "column to check.",
                        "type": "string",
                        "required": True},
                        {
                        "name": "collation",
                        "brief": "collation to check.",
                        "type": "string",
                            "required": True},
                        {
                        "name": "schema",
                        "brief": "Schema to use.",
                        "type": "string",
                            "required": False},
                        {
                        "name": "session",
                        "brief": "The session to be used on the operation.",
                        "type": "object",
                        "classes": ["Session", "ClassicSession"],
                        "required":False}
                    ]
                },
                "collations",
                {
                    "brief": "Collation utilities.",
                    "details": [
                        "A collection of collation utilites"
                    ]
                })

register_plugin("outOfOrder", outoforder_src.out_of_order,
                {
                    "brief": "Find values in a column that becomes out of order for a given collation",
                    "parameters": [
                        {
                        "name": "table",
                        "brief": "Table name to use.",
                        "type": "string",
                        "required": True},
                        {
                        "name": "column",
                        "brief": "column to check.",
                        "type": "string",
                        "required": True},
                        {
                        "name": "collation",
                        "brief": "collation to check.",
                        "type": "string",
                            "required": True},
                        {
                        "name": "schema",
                        "brief": "Schema to use.",
                        "type": "string",
                            "required": False},
                        {
                        "name": "session",
                        "brief": "The session to be used on the operation.",
                        "type": "object",
                        "classes": ["Session", "ClassicSession"],
                        "required":False}
                    ]
                },
                "collations",
                {
                    "brief": "Collation utilities.",
                    "details": [
                        "A collection of collation utilites"
                    ]
                })

