# mysqlsh_plugins_common.py
# -------------------------
# This file holds common code that is shared among the individual plugins
# located in sub-folders.

def register_plugin(func_name, func, info, category, category_info=None):
    """Simplifies the registration of plugins and their member functions

    Args:
        func_name (str): The name of the plugin function, using camelCase
        func (function): The actual plugin function
        info (dict): A List of Dicts with information about the plugin and its 
            paramemters.
            It requires the keys 'name' with the name of the parameter,
            a key 'brief' with a short description of the plugin,
            a key 'type' with the type as string (e.g. "string" or "object"),
            a key 'required', set to True or False depending if the parameter is
            optional. Optional parameters should always be at the end of the 
            parameter list.
        category (str): The name of the category the plugin should be put into.
            Examples: schema, performance, ...
        category_info (dict): A Dict with information about the plugin category. 
            It needs to contain
                - a key 'brief' with a short description,
                - a key 'details' with an array of strings with a longer 
                    description text.
            The keys 'brief' and 'details' are optional after the first call 
            for a given category since they will only be used to create that
            category's extension object.
    
    Returns:
        Nothing
    """
    import mysqlsh
    shell = mysqlsh.globals.shell

    # Check if global object 'ext' has already been registered
    if 'ext' in dir(mysqlsh.globals):
        global_obj = mysqlsh.globals.ext
    else:
        # Otherwise register new global object named 'ext'
        global_obj = shell.create_extension_object()
        shell.register_global("ext", global_obj, 
            { 
                "brief": "MySQL Shell extension plugins.",
                "details": [
                    "This global object is the entry points for MySQL Shell "
                    "extension plugins. Please consolidate the MySQL Shell manual "
                    "to get more information about how to create your own plugins."
                ]
            })

    # Check if the extension object '<category>' has already been created
    try:
        plugin_obj = getattr(global_obj, category)
    except IndexError:
        # If not, add a new extension object to the 'ext' global object
        plugin_obj = shell.create_extension_object()
        shell.add_extension_object_member(global_obj, category, 
            plugin_obj,
            { 
                "brief": category_info['brief'],
                "details": category_info['details']
            })

    # Add the function to the shell extension object as a new member.
    try:
        shell.add_extension_object_member(plugin_obj, func_name, func, info)
    except Exception as e:
        shell.log("ERROR", "Failed to register ext.{0}.{1} ({2}).".
            format(category, func_name, str(e).rstrip()))
