from mysqlsh.plugin_manager import plugin, plugin_function
import mysqlsh

try:
    import pandas as pd

    pandas_present = True
except:
    pandas_present = False
    mysqlsh.globals.shell.log("WARNING",
                              "Python module pandas are not present, check.get_unused_indexes() won't work")

    mysqlsh.globals.shell.log("WARNING",
                              "Try:\n   mysqlsh --pym pip install --user pandas")


def _query_unused_indexes(session):
    if not pandas_present:
        print("Python module pandas are not present, check.get_unused_indexes() won't work")
        print("Try:\n   mysqlsh --py pip install --user pandas")
        return
    exclueded_schemas = ['information_schema',
                         'mysql',
                         'performance_schema',
                         'sys',
                         'mysql_innodb_cluster_metadata']

    filter_string = "WHERE object_schema NOT IN ('{}')".format("','".join(exclueded_schemas))

    stmt = """select object_schema, 
                    object_name, index_name 
             from sys.schema_unused_indexes {}
          """.format(filter_string)

    # maybe can use  pd.read_sql, if that, we need to use sqlalchemy or pymysql

    try:
        result = session.run_sql(stmt)
        result = result.fetch_all()
        if result:
            result = pd.DataFrame([list(index) for index in result], columns=['schema', 'table', 'index'])
        else:
            result = pd.DataFrame(columns=['schema', 'table', 'index'])
    except Exception as e:
        print("Error: {}".format(e))
        return False

    return result


def _query_unused_indexes_group(addresses, user, password):
    shell = mysqlsh.globals.shell
    mysql = mysqlsh.mysql
    pd_common_unused_indexes = pd.DataFrame()

    for address in addresses:
        my_classic_session = mysql.get_session(shell.unparse_uri(
            {'user': user, 'password': password, 'host': address.split(':')[0], 'port': address.split(':')[1]}))

        if pd_common_unused_indexes.empty:
            pd_common_unused_indexes = _query_unused_indexes(my_classic_session)

        if pd_common_unused_indexes is False:
            break

        if pd_common_unused_indexes.empty:
            break

        pd_unused_indexes = _query_unused_indexes(my_classic_session)

        if pd_unused_indexes is False:
            pd_common_unused_indexes = False
            break

        if pd_unused_indexes.empty:
            pd_common_unused_indexes = pd_unused_indexes
            break

        pd_common_unused_indexes = pd.merge(pd_common_unused_indexes, pd_unused_indexes)

    return pd_common_unused_indexes


def _get_common_unused_indexes(session, limit, cluster_object):
    shell = mysqlsh.globals.shell

    current_session_uri = session.get_uri()
    addresses = [instance['address'] for instance in cluster_object.describe()['defaultReplicaSet']['topology']]
    password = shell.prompt("Please Enter password again for {}: ".format(current_session_uri),
                            {'type': 'password'})

    if password is None:
        return

    pd.set_option('display.max_rows', limit)

    result = _query_unused_indexes_group(addresses,
                                         shell.parse_uri(current_session_uri)['user'],
                                         password)

    if result is False:
        return

    if result.empty:
        return

    print(result.head(limit))

    pd_rows = result.shape[0]
    if pd_rows > limit:
        awnser = shell.prompt('Show all unused indexes? ', {'type': 'confirm', 'yes': '&yes', 'defaultValue': 'no'})

        if awnser == '&yes':
            pd.set_option('display.max_rows', pd_rows)
            print(result)
            return


@plugin_function("check.getRstUnusedIndexes")
def get_rs_unused_indexes(limit=20, session=None):
    """
    Print  the unused indexes in a schema or table, or all schemas and tables in a Replica Set

    Args:
        limit (integer): limit the number of rows to display, default 20
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    dba = mysqlsh.globals.dba
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()

        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    try:
        rs = dba.get_replica_set()
        _get_common_unused_indexes(session, limit, rs)

    except Exception as e:
        print("Error: {}".format(e))
        return


@plugin_function("check.getIcUnusedIndexes")
def get_ic_unused_indexes(limit=20, session=None):
    """
    Print  the unused indexes in a schema or table, or all schemas and tables in a cluster.

    Args:
        limit (integer): limit the number of rows to display, default 20
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    dba = mysqlsh.globals.dba
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()

        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    try:
        ic = dba.get_cluster()
        _get_common_unused_indexes(session, limit, ic)

    except Exception as e:
        print("Error: {}".format(e))
        return
