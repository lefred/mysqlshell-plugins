# init.py
# -------

from router import status as router_status
from router.myrouter import MyRouter


router = shell.create_extension_object()


global_obj = shell.create_extension_object()


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
