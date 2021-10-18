import subprocess

def get_sysctl_value(key):
    command = "sysctl %s" % key
    line = subprocess.check_output(command, shell=True).strip() 
    line_sp = line.split()
    return line_sp[2].decode("utf-8")

def print_red_inline(message):
    return("\033[1;31;40m%s\033[0m" % message) 

def print_red(message):
    return("\033[1;31;40m%s\033[0m\n" % message) 

def print_green(message):
    return("\033[1;32;40m%s\033[0m\n" % message) 

def print_orange(message):
    return("\033[1;33;40m%s\033[0m\n" % message) 

def output(val1, val2, indent=0):
    if indent == 0: 
        return "%s: %s\n" % (val1.rjust(30, ' '), val2)
    else:
        return "%s %s: %s\n" % ("|--".rjust(33, " "), val1.rjust(16, ' '), val2)

def run_and_print(title, stmt, session):
    nbrows = 0
    try:
        from prettytable import PrettyTable
    except:
        return(util.print_red("Error importing module prettytable, check if it's installed (ex: mysqlsh --pym pip install --user prettytable)"), nbrows, extra)

    result = session.run_sql(stmt)
    headers=[]
    for col in result.get_columns():
        headers.append(col.get_column_label())

    tab = PrettyTable(headers)
    tab.align = 'r'

    output_str = output(title, "")
    for row in result.fetch_all():
        tab.add_row(row)
        nbrows += 1
    tab.align[result.get_columns()[0].get_column_label()] = 'l'
    output_str += str(tab) + "\n"
    return output_str, nbrows
