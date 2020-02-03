# init.py
# -------
from ext.mysqlsh_plugins_common import register_plugin

from ext.demo import oracle8ball as oracle_8_ball
def hello_world():
    """Simple function that prints "Hello world!"
    Returns:
        Nothing
    """
    print("Hello world!")

def show_schemas(session=None):
    """Lists all database schemas

    Sample function that works either with a session passed as parameter or
    with the global session of the MySQL Shell.

    Args:
        session (object): The optional session object used to query the 
            database. If omitted the MySQL Shell's current session will be used.

    Returns:
        Nothing
    """
    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                "function or connect the shell to a database")
            return
    if session is not None:
        r = session.run_sql("show schemas")
        shell.dump_rows(r)

# Registering the global object 'ext' and the 'demo' extension object
# -------------------------------------------------------------------

# Check if global object 'ext' has already been registered
if 'ext' in globals():
    global_obj = ext
else:
    # If not, register a new global object named 'ext' now
    global_obj = shell.create_extension_object()
    shell.register_global("ext", global_obj, 
        { 
            "brief": "MySQL Shell community plugins.",
            "details": [
                "The global object ext is the entry points for "
                "MySQL Shell extensions."
            ]
        })

# Adding the 'demo' extension object to the 'ext' global object
try:
    plugin_obj = global_obj.demo
except IndexError:
    # If the 'demo' extension object has not been registered yet, do it now
    plugin_obj = shell.create_extension_object()
    shell.add_extension_object_member(global_obj, "demo", plugin_obj,
        {"brief": "A demo plugin that showcases the shell's plugin feature."})

# Add the hello_world function to the 'demo' extension object.
# Please make sure to use camel case naming style for the member's name. The 
# MySQL Shell will automatically translate this to snake case when running in 
# Python mode, e.g. demo.helloWorld() in JS mode vs. demo.hello_world() in 
# Python mode.
try:
    shell.add_extension_object_member(plugin_obj, "helloWorld", hello_world, 
        {"brief": "Prints 'Hello world!'", "parameters": []})
except Exception as e:
    shell.log("ERROR", "Failed to register ext.demo.helloWorld ({0}).".
        format(str(e).rstrip()))

# Add the show_schemas to the 'demo' extension object.
# To define a function parameter as optional, set the 'required' option to False
# when specifying the parameter. Optional parameters should be at the end of
# the parameter list.
try:
    shell.add_extension_object_member(plugin_obj, "showSchemas", show_schemas, 
        {"brief": "Lists all database schemas.",
        "parameters": [{
            "name": "session",
            "brief": "The session to be used on the operation.",
            "type": "object",
            "classes": ["Session", "ClassicSession"],
            "required": False
        }]})
except Exception as e:
    shell.log("ERROR", "Failed to register ext.demo.showSchemas ({0}).".
        format(str(e).rstrip()))


try:
    register_plugin("oracle8ball", oracle_8_ball.tell_me,
    {   
        "brief": "Get the answer from the Oracle 8 Black Ball",
        "parameters": []
    },
        "demo"
    )
except Exception as e:
    shell.log("ERROR", "Failed to register ext.demo.oracle8ball ({0})." . format(str(e).rstrip()))    
