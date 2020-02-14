"""
ardexaplugin
============

:copyright: (c) 2020 Ardexa Pty Limited
:license: MIT, see LICENSE for more details.
"""

import csv
import struct
import time
import os
from io import StringIO
from subprocess import Popen, PIPE

DEBUG = 0
LATEST_FILENAME = "latest.csv"
IS_RUNNING = True
SERVICE_MODE = False
SERVICE_CACHE = {}

def set_debug(debug):
    """Set the debug level across the whole module"""
    # pylint: disable=W0603
    global DEBUG
    DEBUG = debug


def get_ardexa_field_type(field_type):
    """Normalise the type"""
    # pylint: disable=R0911
    field_type = field_type.lower()
    if field_type in ("int", "integer"):
        return "integer"
    if field_type in ("float", "decimal", "real", "freal", "double"):
        return "decimal"
    if field_type in ("bool", "boolean"):
        return "bool"
    if field_type == "date":
        return "date"
    if field_type == "discard":
        return "discard"
    if field_type in ("string", "keyword"):
        return "keyword"
    return None


def process_header_field(field):
    """Convert an object to an Ardexa header"""
    if isinstance(field, str):
        return field

    if not isinstance(field, dict):
        raise ValueError("Unknown header value")

    ardexa_type = get_ardexa_field_type(field['type'])
    if ardexa_type is None:
        raise ValueError("Unknown field type")

    if 'units' in field and field['units'] is not None:
        return "{}({}:{})".format(field['name'], ardexa_type, field['units'])
    return "{}({})".format(field['name'], ardexa_type)


def clean_and_stringify_value(value):
    """This function will escape backslashes since csv.writer does not"""
    value = str(value)
    if value.find("\n") != -1:
        value = value.replace("\n", " ")
    if value.find("\\") != -1:
        value = value.replace("\\", r"\\")
    return value


def make_csv_str(data):
    """Convert list to CSV"""
    output = StringIO()
    csvwriter = csv.writer(output, doublequote=False, escapechar='\\', lineterminator='\n')
    csvwriter.writerow(data)
    data_line = output.getvalue()
    output.close()
    return data_line


def get_log_directory(output_directory, table, source):
    """Generate final log directory.

    Based on the table(string) and source(list). Create the target directory
    if it doesn't exist"""
    log_directory = os.path.join(output_directory, table, *[str(s) for s in source])
    try:
        os.makedirs(log_directory)
    except FileExistsError:
        pass
    return log_directory


def get_output_files(log_directory):
    """Generate absolute file paths for archive and latest"""
    archive_file_name = "{}.csv".format(time.strftime("%Y-%m-%d"))

    latest_file = os.path.join(log_directory, LATEST_FILENAME)
    archive_file = os.path.join(log_directory, archive_file_name)

    return latest_file, archive_file


def write_dyn_log(output_directory, table, source, output, changes_only=False):
    """Turn output into header and data CSV. Append Datetime"""
    header_line = "# Datetime(date)," + make_csv_str([process_header_field(f[0]) for f in output])
    data_line = get_datetime_str() + "," + \
            make_csv_str([clean_and_stringify_value(f[1]) for f in output])
    log_directory = get_log_directory(output_directory, table, source)
    write_log(log_directory, header_line, data_line)


def write_log(log_directory, header_line, data_line):
    """Write the data to the files.

    The ARCHIVE file is the historical log. Files are named according to the
    date.

    The LATEST file is provided so that the Ardexa Agent has a constant point
    of reference for latest events. LATEST is rotated daily.

    Header line is only written to new files.
    """
    write_header = False
    latest_file, archive_file = get_output_files(log_directory)

    if DEBUG > 1:
        print("LATEST : {}".format(latest_file))
        print("ARCHIVE: {}".format(archive_file))
        print("HEADER : {}".format(header_line), end='')
        print("DATA   : {}".format(data_line), end='')

    # If the archive file doesn't exist, means we've moved to a new day.
    # Rotate latest
    if not os.path.isfile(archive_file):
        write_header = True
        if DEBUG > 1:
            print(" * ARCHIVE doesn't exist, rotating LATEST")
        try:
            os.remove(latest_file)
        except FileNotFoundError:
            pass

    # Write the data_line to the log file
    with open(archive_file, "a") as output_archive:
        if write_header:
            output_archive.write(header_line)
        output_archive.write(data_line)

    # And write it to the 'latest'
    with open(latest_file, "a") as output_latest:
        if write_header:
            output_latest.write(header_line)
        output_latest.write(data_line)


def check_pidfile(pidfile, debug):
    """Check that a process is not running more than once, using PIDFILE"""
    # Check PID exists and see if the PID is running
    if os.path.isfile(pidfile):
        pidfile_handle = open(pidfile, 'r')
        # try and read the PID file. If no luck, remove it
        try:
            pid = int(pidfile_handle.read())
            pidfile_handle.close()
            if check_pid(pid, debug):
                return True
        except:
            pass

        # PID is not active, remove the PID file
        os.unlink(pidfile)

    # Create a PID file, to ensure this is script is only run once (at a time)
    pid = str(os.getpid())
    open(pidfile, 'w').write(pid)
    return False


def check_pid(pid, debug):
    """This function will check whether a PID is currently running"""
    try:
        # A Kill of 0 is to check if the PID is active. It won't kill the process
        os.kill(pid, 0)
        if debug > 1:
            print("Script has a PIDFILE where the process is still running")
        return True
    except OSError:
        if debug > 1:
            print("Script does not appear to be running")
        return False


def get_datetime_str():
    """This function gets the local time with local timezone offset"""
    return time.strftime('%Y-%m-%dT%H:%M:%S%z')


def convert_to_int(value):
    """Convert a string to INT"""
    try:
        ret_val = int(value)
        return ret_val, True
    except ValueError:
        return 0, False


def convert_to_float(value):
    """Convert a string to FLOAT"""
    try:
        ret_val = float(value)
        return ret_val, True
    except ValueError:
        return 0.0, False


def convert_int32(high_word, low_word):
    """Convert two words to a 32 bit unsigned integer"""
    return convert_words_to_uint(high_word, low_word)


def convert_words_to_uint(high_word, low_word):
    """Convert two words to a floating point"""
    try:
        low_num = int(low_word)
        # low_word might arrive as a signed number. Convert to unsigned
        if low_num < 0:
            low_num = abs(low_num) + 2**15
        number = (int(high_word) << 16) | low_num
        return number, True
    except:
        return 0, False


def convert_words_to_float(high_word, low_word):
    """Convert two words to a floating point"""
    number, retval = convert_words_to_uint(high_word, low_word)
    if not retval:
        return 0.0, False

    try:
        packed_float = struct.pack('>l', number)
        return struct.unpack('>f', packed_float)[0], True
    except:
        return 0.0, False


def disown(debug):
    """This function will disown, so the Ardexa service can be restarted"""
    # Get the current PID
    pid = os.getpid()
    cgroup_file = "/proc/" + str(pid) + "/cgroup"
    try:
        infile = open(cgroup_file, "r")
    except IOError:
        print("Could not open cgroup file: {}".format(cgroup_file))
        return False

    # Read each line
    for line in infile:
        # Check if the line contains "ardexa.service"
        if line.find("ardexa.service") == -1:
            continue

        # if the lines contains "name=", replace it with nothing
        line = line.replace("name=", "")
        # Split  the line by commas
        items_list = line.split(':')
        accounts = items_list[1]
        dir_str = accounts + "/ardexa.disown"
        # If accounts is empty, continue
        if not accounts:
            continue

        # Create the dir and all subdirs
        full_dir = "/sys/fs/cgroup/" + dir_str
        if not os.path.exists(full_dir):
            os.makedirs(full_dir)
            if debug >= 1:
                print("Making directory: {}".format(full_dir))
        else:
            if debug >= 1:
                print("Directory already exists: {}".format(full_dir))

        # Add the PID to the file
        full_path = full_dir + "/cgroup.procs"
        prog_list = ["echo", str(pid), ">", full_path]
        run_program(prog_list, debug, True)

        # If this item contains a comma, then separate it, and reverse
        # some OSes will need cpuacct,cpu reversed to actually work
        if accounts.find(",") != -1:
            acct_list = accounts.split(',')
            accounts = acct_list[1] + "," + acct_list[0]
            dir_str = accounts + "/ardexa.disown"
            # Create the dir and all subdirs. But it may not work. So use a TRY
            full_dir = "/sys/fs/cgroup/" + dir_str
            try:
                if not os.path.exists(full_dir):
                    os.makedirs(full_dir)
            except:
                continue

            # Add the PID to the file
            full_path = full_dir + "/cgroup.procs"
            prog_list = ["echo", str(pid), ">", full_path]
            run_program(prog_list, debug, True)

    infile.close()

    # For debug purposes only
    if debug >= 1:
        prog_list = ["cat", cgroup_file]
        run_program(prog_list, debug, False)

    # If there are any "ardexa.service" in the proc file. If so, exit with error
    prog_list = ["grep", "-q", "ardexa.service", cgroup_file]
    if run_program(prog_list, debug, False):
        # There are entries still left in the file
        return False

    return True


def run_program(prog_list, debug, shell):
    """Run a  program and check program return code Note that some commands don't work
    well with Popen.  So if this function is specifically called with 'shell=True',
    then it will run the old 'os.system'. In which case, there is no program output
    """
    try:
        if not shell:
            process = Popen(prog_list, stdout=PIPE, stderr=PIPE)
            stdout, stderr = process.communicate()
            retcode = process.returncode
            if debug >= 1:
                print("Program : {}".format(" ".join(prog_list)))
                print("Return Code: {}".format(retcode))
                print("Stdout: {}".format(stdout))
                print("Stderr: {}".format(stderr))
            return bool(retcode)
        command = " ".join(prog_list)
        os.system(command)
        return True
    except:
        return False


def parse_address_list(addrs):
    """Yield each integer from a complex range string like "1-9,12,15-20,23"

    >>> list(parse_address_list('1-9,12,15-20,23'))
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 15, 16, 17, 18, 19, 20, 23]

    >>> list(parse_address_list('1-9,12,15-20,2-3-4'))
    Traceback (most recent call last):
        ...
    ValueError: format error in 2-3-4
    """
    for addr in addrs.split(','):
        elem = addr.split('-')
        if len(elem) == 1: # a number
            yield int(elem[0])
        elif len(elem) == 2: # a range inclusive
            start, end = list(map(int, elem))
            for i in range(start, end+1):
                yield i
        else: # more than one hyphen
            raise ValueError('format error in %s' % addr)
