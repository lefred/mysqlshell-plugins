# init.py
# -------

from proxysql.proxysql import ProxySQL


proxysql = shell.create_extension_object()


global_obj = shell.create_extension_object()


def create(uri):
    my_proxy = ProxySQL(uri)
    return {
         'status': lambda loop=False: my_proxy.get_status(loop),
         'configure': lambda: my_proxy.configure(),
         'hosts': lambda: my_proxy.get_hosts(),
         'version': lambda: my_proxy.get_version(),
         'hostgroups': lambda: my_proxy.get_hostgroups(),
         'getUsers': lambda hostgroup="": my_proxy.get_user_hostgroup(hostgroup),
         'setUser': lambda hostgroup="", user="", password=False: my_proxy.set_user_hostgroup(hostgroup,user,password),
         'importUsers': lambda hostgroup="", user_search="": my_proxy.import_users(hostgroup, user_search),
         'setUserHostgroup': lambda hostgroup="", user_search="": my_proxy.set_host_group(hostgroup, user_search)
    }

shell.add_extension_object_member(proxysql, 'create', lambda uri=False:create(uri),
            {
                'brief':'Create the ProxySQL Object', 'details':['It has ProxySQL methods.'],
                'parameters':[
                                {'name':'uri', 'type':'string', 'required':True, 'brief':'connection uri to ProxySQL Admin port'},
                        ]
                }
            )

shell.add_extension_object_member(global_obj, 'proxysql', proxysql,
{
    'brief':'ProxySQL Object',
    'details':['ProxySQL Object.']
})
