"""
ardexaplugin
============

:copyright: (c) 2018 Ardexa Pty Limited
:license: MIT, see LICENSE for more details.
"""

from __future__ import print_function, unicode_literals
import os
import time
import struct
from subprocess import Popen, PIPE

def write_log(log_directory, log_filename, header, logline, debug,
              require_latest, latest_directory, latest_filename):
    """This function logs a line of data to both a 'log' file, and a 'latest'
    file The 'latest' file is optional, and is sent to this function as a
    boolean value via the variable 'require_latest'.
    So the 2 log directories and filenames are:
            a. (REQUIRED): log_directory + log_filename
            b. (OPTIONAL): latest_directory + latest_filename

    The 'latest' directory and filename is provided so as to have a consistent
    file of the latest events This is usually the latest day of events.
    The way this function works with the 'latest' log_dir is as follows:
            a. It checks for the existance of log_directory + log_filename
            b. If (a) doesn't exist, then any 'latest' file is removed and a new one created
            c. If (a) already exists, logs are written to any existing 'latest' file
                    If one doesn't exist, it will be created

    For both the 'log' and 'latest' files, a header line will be written if a new
    file is created Please note that a header must start with the '#' symbol, so
    the Ardexa agent can interpret this line as a header , and will not send it to
    the cloud
    """
    create_new_file = False

    # Make sure the logging directory exists. The following will create all the necessary subdirs,
    # if the subdirs exist in part or in full
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    full_path_log = os.path.join(log_directory, log_filename)
    if debug > 1:
        print("Full path of log directory: ", full_path_log)
    # If the file doesn't exist, annotate that a new 'latest' file is to be created
    # and that a header is to be created
    if not os.path.isfile(full_path_log):
        if debug > 1:
            print("Log file doesn't exist: ", full_path_log)
        create_new_file = True

    # Repeat for the 'latest', if it doesn't exist
    if require_latest:
        if not os.path.exists(latest_directory):
            os.makedirs(latest_directory)
        full_path_latest = os.path.join(latest_directory, latest_filename)
        if debug > 1:
            print("Full path of latest directory: ", full_path_latest)
        # If the 'create_new_file' tag is set AND the file exists, then remove it
        if create_new_file and os.path.isfile(full_path_latest):
            # then remove the file
            os.remove(full_path_latest)

    # Now create both (or open both) and write to them
    if debug > 1:
        print("##########################################")
        print("Writing the line to", full_path_latest)
        print(logline)
        print("##########################################")

    # Write the logline to the log file
    output_file = open(full_path_log, "a")
    if create_new_file:
        output_file.write(header)
    output_file.write(logline)
    output_file.close()

    # And write it to the 'latest' if required
    if require_latest:
        write_latest = open(full_path_latest, "a")
        if create_new_file:
            write_latest.write(header)
        write_latest.write(logline)
        write_latest.close()


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
        print("Could not open cgroup file: ", cgroup_file)
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
                print("Making directory: ", full_dir)
        else:
            if debug >= 1:
                print("Directory already exists: ", full_dir)

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
                print("Program : ", " ".join(prog_list))
                print("Return Code: ", retcode)
                print("Stdout: ", stdout)
                print("Stderr: ", stderr)
            return bool(retcode)
        else:
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
