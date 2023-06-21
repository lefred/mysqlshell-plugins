# init.py
# -------
from mysqlsh.plugin_manager import plugin, plugin_function
from enum import Enum
import os

class Operation(Enum):
    START = 1
    STOP = 2
    KILL = 3
    DELETE = 4

def _get_all_sandboxes(sandboxdir):
    sandboxes = []

    for root, dirs, files in os.walk(sandboxdir):
        for file in files:
            if (file == "mysqld"):
                basedir = os.path.dirname(root)
                sandboxport = os.path.basename(os.path.normpath(basedir))
                allowed_port_range = range(1024, 65535)
                if (sandboxport.isdigit() and int(sandboxport) in allowed_port_range):
                    sandboxes.append(sandboxport)

    return sandboxes

def _sandbox_op(op, sandbox):
    op_infinitive = ""
    op_verb = ""

    if (op == Operation.START):
        op_infinitive = "Starting"
        op_verb = "start"
    elif (op == Operation.STOP):
        op_infinitive = "Stopping"
        op_verb = "stop"
    elif (op == Operation.KILL):
        op_infinitive = "Killing"
        op_verb = "kill"
    elif (op == Operation.DELETE):
        op_infinitive = "Deleting"
        op_verb = "delete"

    print("%s sandbox running in port %s" % (op_infinitive, sandbox))

    try:
        if (op == Operation.START):
            dba.start_sandbox_instance(sandbox)
        elif (op == Operation.STOP):
            dba.stop_sandbox_instance(sandbox)
        elif (op == Operation.KILL):
            dba.kill_sandbox_instance(sandbox)
        elif (op == Operation.DELETE):
            dba.delete_sandbox_instance(sandbox)
    except Exception as err:
        print("Failed to %s sandbox '%s': %s" % (op_verb, sandbox, err))


@plugin
class sandboxes:
    """
    MySQL Shell Sandboxes management.

    A collection of utilities to manage MySQL Shell Sandboxes.
    """

@plugin_function("sandboxes.list")
def list(sandboxdir=None):
    """
    Lists all Sandboxes deployed on the sandboxDir.

    Args:
        sandboxdir (string): The sandboxDir used to search for existing sandboxes.

    Returns:
        A list of sandboxes deployed on sandboxdir
    """

    if sandboxdir is None:
        sandboxdir = shell.options.sandboxDir

    print("Current sandboxdir is '" + sandboxdir + "'")

    sandboxes = _get_all_sandboxes(sandboxdir)

    if not sandboxes:
        print("No sandboxes found")
    else:
        print("Sandboxes found: ", _get_all_sandboxes(sandboxdir))

@plugin_function("sandboxes.killAll")
def killAll(sandboxdir=None):
    """
    Kills all Sandboxes deployed on the sandboxDir.

    Args:
        sandboxdir (string): The sandboxDir used to search for existing sandboxes.
    """

    if sandboxdir is None:
        sandboxdir = shell.options.sandboxDir

    sandboxes = _get_all_sandboxes(sandboxdir)

    for sandbox in sandboxes:
        _sandbox_op(Operation.KILL, sandbox)

@plugin_function("sandboxes.stopAll")
def stopAll(sandboxdir=None):
    """
    Stops all Sandboxes deployed on the sandboxDir.

    Args:
        sandboxdir (string): The sandboxDir used to search for existing sandboxes.
    """

    if sandboxdir is None:
        sandboxdir = shell.options.sandboxDir

    sandboxes = _get_all_sandboxes(sandboxdir)

    for sandbox in sandboxes:
        _sandbox_op(Operation.STOP, sandbox)


@plugin_function("sandboxes.startAll")
def startAll(sandboxdir=None):
    """
    Starts all Sandboxes deployed on the sandboxDir.

    Args:
        sandboxdir (string): The sandboxDir used to search for existing sandboxes.
    """

    if sandboxdir is None:
        sandboxdir = shell.options.sandboxDir

    sandboxes = _get_all_sandboxes(sandboxdir)

    for sandbox in sandboxes:
        _sandbox_op(Operation.START, sandbox)

@plugin_function("sandboxes.deleteAll")
def deleteAll(force=False, sandboxdir=None):
    """
    Deletes all Sandboxes deployed on the sandboxDir.

    Args:
        force (bool): Attempt to kill the sandbox before deleting it
        sandboxdir (string): The sandboxDir used to search for existing sandboxes.
    """

    if sandboxdir is None:
        sandboxdir = shell.options.sandboxDir

    sandboxes = _get_all_sandboxes(sandboxdir)

    for sandbox in sandboxes:
        if force:
            _sandbox_op(Operation.KILL, sandbox)

        _sandbox_op(Operation.DELETE, sandbox)

    print("All sandboxes successfully deleted")
