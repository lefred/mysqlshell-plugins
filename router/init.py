# init.py
# -------

from ext.mysqlsh_plugins_common import register_plugin
from ext.router import status as router_status
from ext.router.myrouter import MyRouter


router = shell.create_extension_object()


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


def create(uri):
    my_router = MyRouter(uri)
    return { 
         'connections': lambda route_to_find="": my_router.connections(route_to_find), 
         'status': lambda: my_router.status(), 
         'api': my_router.api
    }
                        
shell.add_extension_object_member(router, 'create', lambda uri=False:create(uri), 
            {
                'brief':'Create the MySQL Router Object', 'details':['It has MyRouter methods.'], 
                'parameters':[
                                {'name':'uri', 'type':'string', 'required':True, 'brief':'connection uri to ProxySQL Admin port'},
                        ]
                }
            )

shell.add_extension_object_member(global_obj, 'router', router, 
{
    'brief':'MySQL Router Object', 
    'details':['MySQL Router Object.']
})
