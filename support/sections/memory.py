
import subprocess
from support.sections import util

def get_linux_memory_usage(advices):
    command = "free -h"
    all_info = subprocess.check_output(command, shell=True).strip()                  
    for line in all_info.decode("utf-8").split("\n"): 
        if "Mem:" in line:
            line_sp = line.split()
            memory =  util.output("Total Memory", line_sp[1])
            memory += util.output("Memory Used", line_sp[2])
            memory += util.output("Memory Free", line_sp[3])
            memory += util.output("Filesystem Cache", line_sp[5])
        if "Swap:" in line:
            line_sp = line.split()
            memory += util.output("Total Swap", line_sp[1])
            memory += util.output("Swap Used", line_sp[2])
            memory += util.output("Swap Free", line_sp[3])
            swappiness = util.get_sysctl_value('vm.swappiness')
            memory += util.output("Swappiness", swappiness)
            if advices:
                if int(swappiness) <= 10 and int(swappiness) > 0:
                    memory += util.print_green("\nYour swappiness value is good")
                elif int(swappiness) == 0:
                    memory += util.print_red("0 as swappiness is dangerous, you should set it to 5")
                else:
                    memory += util.print_red("Your swappiness value is to high, you should set it to a value between 1 and 10")
    return memory