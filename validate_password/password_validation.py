from mysqlsh.plugin_manager import plugin, plugin_function
import mysqlsh
import time

shell = mysqlsh.globals.shell
dba = mysqlsh.globals.dba

def _check_plugin(plugin_name):
    result = shell.get_session().run_sql("select plugin_name from information_schema.plugins")
    if result.has_data():
       for i in result.fetch_all():
           if plugin_name in str(list(i)):
                print("\n\033[1mINFO:\033[0m Plugin " + plugin_name + " is already installed \n")
                return True
    return False

def _install_plugin(plugin_name, plugin_file):
    if not _check_plugin(plugin_name):
        try:
            if _check_super_read_only() == "ON":
                shell.get_session().run_sql("set global super_read_only=off")
                shell.get_session().run_sql("install plugin " + plugin_name + " soname '" + plugin_file + "'")
                shell.get_session().run_sql("set global super_read_only=on")
            else:
                shell.get_session().run_sql("install plugin " + plugin_name + " soname '" + plugin_file + "'")
            return True
        except:
            print("\n\033[1mERROR:\033[0m Unable to install plugin " + plugin_name + " on " + shell.parse_uri(shell.get_session().get_uri())['host'] + ":" + str(shell.parse_uri(shell.get_session().get_uri())['port']) + "\n")
            return False
    else:
        return True

def _check_super_read_only():
    return str(list(shell.get_session().run_sql("show variables like 'super_read_only'").fetch_all()[0])).strip("[']").strip("', 'super_read_only")

def _list_secondary_nodes():
    result = shell.get_session().run_sql("select concat(member_host,':',member_port) from performance_schema.replication_group_members where member_role<>'PRIMARY'")
    list_output = []
    if result.has_data():
        for row in result.fetch_all():
            list_output.append(str(list(row)).strip("[']"))
        return list_output
    else:
        return '0' 

def _get_plugin_file_location():
    return str(list(shell.get_session().run_sql("show variables like 'lc_messages_dir'").fetch_all()[0])).strip("[']").strip("', 'lc_messages_dir")

def _set_persist_all_nodes(dbUser, dbPassword, variable_name, variable_value):
    x = shell.get_session()
    result = _list_secondary_nodes()
    try:
        if len(result) > 0:
            for row in range(len(result)):
                y = shell.open_session(dbUser + "@" + result[row], dbPassword)
                shell.set_session(y)
                try:
                    shell.get_session().run_sql("set persist " + variable_name + "='" + variable_value + "'")
                except:
                    shell.get_session().run_sql("set persist " + variable_name + "=" + variable_value)
        shell.set_session(x)
        try:
            shell.get_session().run_sql("set persist " + variable_name + "='" + variable_value +"'")
        except:
            shell.get_session().run_sql("set persist " + variable_name + "=" + variable_value)
        return True
    except:
        print("\n\033[1mERROR:\033[0m Unable to persist " + variable_name + " with variable value = " + variable_value)
        shell.set_session(x)
        return False

def _install_password_validation_plugin(dbUser):
    x = shell.get_session()
    result = _list_secondary_nodes()
    try:
        if len(result) > 0:
            dbPassword = shell.prompt("Please provide the password for '" + dbUser + "' : ", {'type':'password'})
            for row in range(len(result)):
                y = shell.open_session(dbUser + "@" + result[row], dbPassword)
                shell.set_session(y)
                if _install_plugin('validate_password','validate_password.so'):
                    print("\n\033[92mINFO:\033[0m plugin installed on " + result[row])
        shell.set_session(x)
        _install_plugin('validate_password','validate_password.so')
    except:
        print("\n\033[1mERROR:\033[0m: 'Validate Password' plugin installation is ABORTED !")
        shell.set_session(x)
        return
    print("\n\033[1mINFO:\033[0m 'Validate Password' plugin is installed successfully\n")

def _show_variables(variable_name):
    result = shell.get_session().run_sql("show variables like '" + variable_name + "'")
    list_output = []
    if result.has_data():
        for row in result.fetch_all():
            list_output.append(str(list(row)).strip("[']").strip(variable_name).strip("', '"))
        return list_output[0]
    else:
        return '0'

def _check_if_system_variables_admin_assigned():
    result = shell.get_session().run_sql("show grants")
    if result.has_data():
        for i in result.fetch_all():
            if ("SYSTEM_VARIABLES_ADMIN" in str(list(i))) or ("SUPER" in str(list(i))):
                return True
    return False

def _check_if_cluster_admin_is_required():
    result = shell.get_session().run_sql("select count(1) from performance_schema.replication_group_members")
    for i in result.fetch_all():
        return str(list(i)).strip("[]")

def _check_if_user_has_cluster_admin():
    result = shell.get_session().run_sql("show grants")
    if result.has_data():
        for i in result.fetch_all():
            if "mysql_innodb_cluster_metadata" in str(list(i)):
                return True
    return False

def _check_if_current_user_have_privilege():
    result = shell.get_session().run_sql("show grants")
    if result.has_data():
        for i in result.fetch_all():
            if (("*.*" in str(list(i))) or ("`mysql`.*" in str(list(i)))):
                if "INSERT" in str(list(i)):
                    if _check_if_system_variables_admin_assigned():
                        if _check_if_cluster_admin_is_required() != "0":
                            if _check_if_user_has_cluster_admin():
                                return True
                        else:
                            return True
    return False
           
def _master_checks():
    if _check_super_read_only() == 'ON':
        print("\n\033[1mERROR:\033[0m unable to continue because super-read-only is ENABLED \n")
        return False
    if not _check_if_current_user_have_privilege():
        print("\n\033[1mERROR:\033[0m username may not have enough privilege to install a plugin or change system variables")
        print("\n\033[1mINFO:\033[0m If MySQL InnoDB Cluster or Group Replication, please ensure to use Cluster Admin to connect \n")
        return False
    return True

def _set_validate_password_check_user_name(dbUser, dbPassword):
    print("\n\033[96mVariable Name : validate_password_check_user_name\033[0m")
    print(shell.parse_uri(shell.get_session().get_uri())['host'] + ":" + str(shell.parse_uri(shell.get_session().get_uri())['port']) + " = " + _show_variables('validate_password_check_user_name'))
    x = shell.get_session()
    result = _list_secondary_nodes()
    try:
        if len(result) > 0:
             for row in range(len(result)):
                try:
                    y = shell.open_session(dbUser + "@" + result[row], dbPassword)
                    shell.set_session(y)
                    print(shell.parse_uri(shell.get_session().get_uri())['host'] + ":" + str(shell.parse_uri(shell.get_session().get_uri())['port']) + " = " + _show_variables('validate_password_check_user_name'))
                except:
                    print("\n\033[1mINFO:\033[0m Unable to connect to " + result[row] + ", SKIPPED !")
        shell.set_session(x)
        new_value = shell.prompt("Enter new value for validate_password_check_user (1=ON, 2=OFF, 0=Unchanged) : ", {'defaultValue':'0'})
        if new_value != '0':
            if new_value == '1':
                if _set_persist_all_nodes(dbUser, dbPassword, 'validate_password_check_user_name','ON'):
                    print("\n\033[1mINFO:\033[0m validate_password_check_user_name value is 'ON' \n")
            if new_value == '2':
               if _set_persist_all_nodes(dbUser, dbPassword, 'validate_password_check_user_name','OFF'):
                    print("\n\033[1mINFO:\033[0m validate_password_check_user_name value is 'OFF' \n")
        else:
            print("\n\033[1mINFO:\033[0m validate_password_check_user_name value is UNCHANGED !\n")
    except:
        print("\n\033[1mERROR:\033[0m unable to set validate_password_check_user_name !")

def _check_numeric(new_value):
    try:
        z = int(new_value)
        return True
    except:
        print("\033[1mERROR:\033[0m INVALID NUMERIC Value, SKIPPED ! \n")
        return False

def _set_validate_password_length(dbUser, dbPassword):
    print("\n\033[96mVariable Name : validate_password_length\033[0m")
    print(shell.parse_uri(shell.get_session().get_uri())['host'] + ":" + str(shell.parse_uri(shell.get_session().get_uri())['port']) + " = " + _show_variables('validate_password_length'))
    x = shell.get_session()
    result = _list_secondary_nodes()
    try:
        if len(result) > 0:
             for row in range(len(result)):
                try:
                    y = shell.open_session(dbUser + "@" + result[row], dbPassword)
                    shell.set_session(y)
                    print(shell.parse_uri(shell.get_session().get_uri())['host'] + ":" + str(shell.parse_uri(shell.get_session().get_uri())['port']) + " = " + _show_variables('validate_password_length'))
                except:
                    print("\n\033[1mINFO:\033[0m Unable to connect to " + result[row] + ", SKIPPED !")
        shell.set_session(x)
        new_value = shell.prompt("Enter new NUMERIC value for validate_password_length (default: 8) : ", {'defaultValue':'8'})
        if _check_numeric(new_value):
            if _set_persist_all_nodes(dbUser, dbPassword, 'validate_password_length',new_value):
                print("\n\033[1mINFO:\033[0m validate_password_check_user_name value is '" + new_value + "' \n")
    except:
        print("\n\033[1mERROR:\033[0m unable to set validate_password_check_user_name !")

def _set_validate_password_policy(dbUser, dbPassword):
    print("\n\033[96mVariable Name : validate_password_policy\033[0m")
    print(shell.parse_uri(shell.get_session().get_uri())['host'] + ":" + str(shell.parse_uri(shell.get_session().get_uri())['port']) + " = " + _show_variables('validate_password_policy'))
    x = shell.get_session()
    result = _list_secondary_nodes()
    try:
        if len(result) > 0:
             for row in range(len(result)):
                try:
                    y = shell.open_session(dbUser + "@" + result[row], dbPassword)
                    shell.set_session(y)
                    print(shell.parse_uri(shell.get_session().get_uri())['host'] + ":" + str(shell.parse_uri(shell.get_session().get_uri())['port']) + " = " + _show_variables('validate_password_policy'))
                except:
                    print("\n\033[1mINFO:\033[0m Unable to connect to " + result[row] + ", SKIPPED !")
        shell.set_session(x)
        new_value = shell.prompt("Enter new value for validate_password_policy (0=LOW, 1=MEDIUM, 2=HIGH) : ", {'defaultValue':'1'})
        if _set_persist_all_nodes(dbUser, dbPassword, 'validate_password_policy',new_value):
            shell.set_session(x)    
            print("\n\033[1mINFO:\033[0m validate_password_policy value is '" + _show_variables('validate_password_policy') + "' \n")
        _set_validate_password_length(dbUser,dbPassword)
        return new_value
    except:
        print("\n\033[1mERROR:\033[0m unable to set validate_password_check_user_name !")
        shell.set_session(x)
        return "0"

def _set_validate_password_others(dbUser, dbPassword):
    print("\n\033[96mVariable Name : \033[0m")
    print("- validate_password_mixed_case_count")
    print("- validate_password_number_count")
    print("- validate_password_special_char_count")
    print("- validate_password_dictionary_file \n")
    
    mixed_case_count_value = shell.prompt("Enter new NUMERIC value for validate_password_mixed_case_count (default: 1) : ", {'defaultValue':'1'})
    if not _check_numeric(mixed_case_count_value):
        mixed_case_count_value = '1'

    number_count_value = shell.prompt("Enter new NUMERIC value for validate_password_number_count (default: 1) : ", {'defaultValue':'1'})
    if not _check_numeric(number_count_value):
        number_count_value = '1'

    special_char_count_value = shell.prompt("Enter new NUMERIC value for validate_password_special_char_count (default: 1) : ", {'defaultValue':'1'})
    if not _check_numeric(special_char_count_value):
        mixed_case_count_value = '1'

    dictionary_file = shell.prompt("Enter new value for validate_password_dictionary_file (default: '') : ", {'defaultValue':''})

    if _set_persist_all_nodes(dbUser, dbPassword, 'validate_password_mixed_case_count', mixed_case_count_value):
        print("\n\033[1mINFO:\033[0m validate_password_mixed_case_count value is '" + mixed_case_count_value + "' \n")
    
    if _set_persist_all_nodes(dbUser, dbPassword, 'validate_password_number_count', number_count_value):
        print("\n\033[1mINFO:\033[0m validate_password_mixed_case_count value is '" + number_count_value + "' \n")

    if _set_persist_all_nodes(dbUser, dbPassword, 'validate_password_special_char_count', special_char_count_value):
        print("\n\033[1mINFO:\033[0m validate_password_special_char_count value is '" + special_char_count_value + "' \n")

    if _set_persist_all_nodes(dbUser, dbPassword, 'validate_password_dictionary_file', dictionary_file):
        print("\n\033[1mINFO:\033[0m validate_password_dictionary_file value is '" + dictionary_file + "' \n")

@plugin_function("validate_password.installPlugin")
def installPlugin():
    """
    VALIDATE_PASSWORD Plugin Installation

    A function to:
    
        Install VALIDATE_PASSWORD plugin on the stand-alone database, SOURCE-REPLICA, and InnoDB Cluster / Group Replication

        If Target Databases are MySQL InnoDB Cluster or Group Replication, this function will install VALIDATE_PASSWORD plugin automatically on ALL Nodes
    
    """
    if _master_checks() and not _check_plugin('validate_password'):
        _install_password_validation_plugin(shell.parse_uri(shell.get_session().get_uri())['user'])

@plugin_function("validate_password.setPolicy")
def setPolicy():
    """
    PASSWORD VALIDATION Set Policy

    A function to:
    
        set PERSIST VALIDATE_PASSWORD_* variables on the stand-alone database, SOURCE-REPLICA, and InnoDB Cluster / Group Replication

        If Target Databases are MySQL InnoDB Cluster or Group Replication, this function will set PERSIST VALIDATE_PASSWORD_* variables automatically on ALL Nodes
    
    """
    if _master_checks():
        if _check_plugin('validate_password'):
            result = _list_secondary_nodes()
            dbUser = shell.parse_uri(shell.get_session().get_uri())['user']
            if len(result) > 0:
                dbPassword = shell.prompt("Please provide the password for '" + dbUser + "' : ", {'type':'password'})
            else:
                dbPassword = ""
            _set_validate_password_check_user_name(dbUser, dbPassword)
            if _set_validate_password_policy(dbUser, dbPassword) != "0":
                _set_validate_password_others(dbUser, dbPassword)
        else:
            print("\n\033[1mERROR:\033[0m Validate Password plugin is NOT INSTALLED\n")
