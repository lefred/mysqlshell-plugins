
import subprocess
from support.sections import util


def _get_mount_options(disk):
    command = "grep '%s' /proc/mounts" % disk
    all_info = subprocess.check_output(command, shell=True).strip()                  
    for line in all_info.decode("utf-8").split("\n"):  
        line_sp = line.split()
        file_system = line_sp[2]
        attributes = line_sp[3]
    return file_system, attributes

def _get_physical_info(path):
    command = "df -h %s | tail -n 1" % path
    all_info = subprocess.check_output(command, shell=True).strip()
    physical_disk = ""
    for line in all_info.decode("utf-8").split("\n"): 
        line_sp = line.split()
        physical_disk = line_sp[0]
        output  = util.output("Physical drive", physical_disk,1)
        output += util.output("Size", line_sp[1],1)
        output += util.output("Used (%s)" % line_sp[4], line_sp[2],1)
        output += util.output("Free", line_sp[3],1)
        output += util.output("Mount point", line_sp[5],1)
        filesystem, attributes = _get_mount_options(line_sp[0])
        output += util.output("Filesystem", filesystem,1)
        output += util.output("Mount attributes", attributes,1)
    return output, physical_disk

def get_linux_disk_info(datadirs):
    #get the info related to datadir
    datadir = datadirs['@@datadir']
    output =  util.output("Datadir", datadir)
    output2, datadir_physical = _get_physical_info(datadir)
    output += output2
    for key in datadirs:

        if key == "@@datadir":
            continue        
        output += util.output(key[2:], datadirs[key])
        if datadirs[key] == "./" or datadirs[key] == datadir or not datadirs[key]:
            continue
        else:
            if datadirs[key].startswith("./"):
                path_to_check = datadir + datadirs[key][1:]
            else:
                path_to_check = datadirs[key]

            output2, physical_disk2 = _get_physical_info(path_to_check)

            if datadir_physical != physical_disk2:
                output += output2
            
    return output