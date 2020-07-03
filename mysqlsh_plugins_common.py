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

def is_consumer_enabled(event_name, session, shell):
     
    stmt = """SELECT NAME, ENABLED FROM performance_schema.setup_consumers  
              WHERE NAME LIKE '{}' AND ENABLED='NO';""".format(event_name)
    result = session.run_sql(stmt)
    consumers = result.fetch_all()
    ok = False
    if len(consumers) > 0:
        consumers_str = ""
        for consumer in consumers:
            consumers_str += "%s, " % consumer[0]

        answer = shell.prompt("""Some consumers are not enabled: %s 
Do you want to enabled them now ? (y/N) """
                              % consumers_str, {'defaultValue':'n'})
        if answer.lower() == 'y':
            stmt = """UPDATE performance_schema.setup_consumers
                      SET ENABLED = 'YES'
                      WHERE NAME LIKE '{}'
                      AND ENABLED='NO'""".format(event_name)
            result = session.run_sql(stmt)
            ok = True
    else:
        ok = True
    
    return ok

def are_instruments_enabled(instrument_name, session, shell):    

    stmt = """SELECT NAME, ENABLED
         FROM performance_schema.setup_instruments
        WHERE NAME LIKE '{}'
              AND ENABLED='NO'""".format(instrument_name)
    result = session.run_sql(stmt)
    instruments = result.fetch_all()
    ok = False
    if len(instruments) > 0:
        instruments_str = ""
        for instrument in instruments:
            instruments_str += "%s, " % instrument[0]

        answer = shell.prompt("""Some instruments are not enabled: %s 
Do you want to enabled them now ? (y/N) """
                              % instruments_str, {'defaultValue':'n'})
        if answer.lower() == 'y':
            stmt = """UPDATE performance_schema.setup_instruments
                      SET ENABLED = 'YES', TIMED = 'YES'
                      WHERE NAME LIKE '{}'
                      AND ENABLED='NO'""".format(instrument_name)
            result = session.run_sql(stmt)
            ok = True
    else:
        ok = True
    
    return ok

def run_and_show(stmt, session=None):
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    result = session.run_sql(stmt)
    shell.dump_rows(result)
    return