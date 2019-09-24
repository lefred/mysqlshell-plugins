# init.py
# -------

from ext.mysqlsh_plugins_common import register_plugin
from ext.proxysql.proxysql import ProxySQL


proxysql = shell.create_extension_object()


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


def create(ip, port, user, password):
    my_proxy = ProxySQL(ip, port, user, password)
    return { 
         'connections': lambda route_to_find="": my_proxy.connections(route_to_find), 
         'status': lambda loop=False: my_proxy.get_status(loop), 
         'configure': lambda: my_proxy.configure(),
         'hosts': lambda: my_proxy.get_hosts(),
         'version': lambda: my_proxy.get_version(),
         'hostgroups': lambda: my_proxy.get_hostgroups(),
         'getUsers': lambda hostgroup="": my_proxy.get_user_hostgroup(hostgroup),
         'setUser': lambda hostgroup="", user="", password=False: my_proxy.set_user_hostgroup(hostgroup,user,password),
    }
                        
shell.add_extension_object_member(proxysql, 'create', lambda ip, port, user, password=False:create(ip, port, user, password), 
            {
                'brief':'Create the ProxySQL Object', 'details':['It has ProxySQL methods.'], 
                'parameters':[
                                {'name':'ip', 'type':'string', 'required':True, 'brief':'ip'},
                                {'name':'port', 'type':'integer', 'required':True, 'brief':'port'},
                                {'name':'user', 'type':'string', 'required':True, 'brief':'user'},
                                {'name':'password', 'type':'string', 'required':False, 'brief':'password'}
                        ]
                }
            )

shell.add_extension_object_member(global_obj, 'proxysql', proxysql, 
{
    'brief':'ProxySQL Object', 
    'details':['ProxySQL Object.']
})
