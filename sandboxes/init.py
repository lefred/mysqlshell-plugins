# init.py
# -------
from mysqlsh.plugin_manager import plugin, plugin_function
from enum import Enum
import os, json, sys

class Operation(Enum):
    START = 1
    STOP = 2
    KILL = 3
    DELETE = 4

def _get_sandbox_basedir(sandboxdir, port):
    basedir = os.path.join(sandboxdir, str(port))
    bindir = os.path.join(basedir, "bin")
    mysqld = os.path.join(bindir, "mysqld")
    if not os.path.exists(mysqld):
        raise Exception("Sandbox at port %s was not found" % port)
    return basedir

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

def _component_library_name(component):
    """
    Returns the library filename expected for a given component name.
    """
    if sys.platform.startswith("linux"):
        return component + ".so"
    elif sys.platform == "darwin":
        return component + ".dylib"
    elif sys.platform == "win32":
        return component + ".dll"
    else:
        raise Exception("Unsupported OS for component validation")


def _get_plugin_dir_for_sandbox(port):
    """
    Connects to the sandbox instance and retrieves @@plugin_dir.
    """
    uri = f"mysql://root@localhost:{port}"
    try:
        session = shell.open_session(uri)
    except Exception as err:
        raise Exception(f"Cannot connect to sandbox on port {port}: {err}")

    try:
        res = session.run_sql("SELECT @@plugin_dir")
        row = res.fetch_one()
        if row is None:
            raise Exception("Could not fetch @@plugin_dir")
        return row[0]
    finally:
        session.close()


def _validate_component_exists(port, component):
    """
    Validates that the component library exists inside @@plugin_dir.
    """
    print(f"Validating if component exists...")
    plugin_dir = _get_plugin_dir_for_sandbox(port)
    libname = _component_library_name(component)
    fullpath = os.path.join(plugin_dir, libname)

    return os.path.exists(fullpath), fullpath, plugin_dir


@plugin_function("sandboxes.addComponentToManifest")
def addComponentToManifest(port, component, sandboxdir=None):
    """
    Adds a component entry to the mysqld.my manifest of a sandbox.

    Args:
        port (int): Sandbox port
        component (string): Component name, e.g. 'component_keyring_file'
        sandboxdir (string): The sandboxDir used to search for existing sandboxes.
    """

    if sandboxdir is None:
        sandboxdir = shell.options.sandboxDir

    # Validate sandbox exists
    basedir = _get_sandbox_basedir(sandboxdir, port)
    bindir = os.path.join(basedir, "bin")
    manifest_path = os.path.join(bindir, "mysqld.my")

    # Validate component
    exists, libpath, plugin_dir = _validate_component_exists(port, component)

    if not exists:
        print(f"ERROR: Component '{component}' not found for sandbox {port}.")
        print(f"Expected library: {os.path.basename(libpath)}")
        print(f"Searched in plugin_dir = {plugin_dir}")
        print(f"Full path checked: {libpath}")
        return

    # Write manifest
    manifest_data = { "components": f"file://{component}" }

    try:
        with open(manifest_path, "w") as f:
            json.dump(manifest_data, f, indent=2)
        print(f"Manifest written successfully to: {manifest_path}")
        print(f"Component validated in plugin_dir: {plugin_dir}")
    except Exception as err:
        print(f"Failed to write manifest: {err}")
