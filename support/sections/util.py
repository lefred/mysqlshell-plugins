import subprocess

def get_sysctl_value(key):
    command = "sysctl %s" % key
    line = subprocess.check_output(command, shell=True).strip() 
    line_sp = line.split()
    return line_sp[2].decode("utf-8")


def print_red(message):
    return("\033[1;31;40m%s\033[0m\n" % message) 

def print_green(message):
    return("\033[1;32;40m%s\033[0m\n" % message) 

def output(val1, val2, indent=0):
    if indent == 0: 
        return "%s: %s\n" % (val1.rjust(30, ' '), val2)
    else:
        return "%s %s: %s\n" % ("|--".rjust(33, " "), val1.rjust(16, ' '), val2)