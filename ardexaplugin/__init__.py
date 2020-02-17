"""
ardexaplugin
============

:copyright: (c) 2020 Ardexa Pty Limited
:license: MIT, see LICENSE for more details.
"""

import csv
import signal
import struct
import sys
import time
import os
from multiprocessing import Event, Process
from io import StringIO
from subprocess import Popen, PIPE

DEBUG = 0
LATEST_FILENAME = "latest.csv"
SERVICE_MODE = False
SERVICE_CACHE = {}


def set_debug(debug):
    """Set the debug level across the whole module"""
    # pylint: disable=W0603
    global DEBUG
    DEBUG = debug


def parse_service_file(command_file, file_args=None):
    """Open and parse CSV service file"""
    commands = []
    line_number = 1
    for row in csv.DictReader(command_file, skipinitialspace=True):
        line_number += 1
        try:
            flags = row.pop('flags', None)
            frequency = int(row.pop('frequency', None))
            for flag in flags.split(':'):
                if flag:
                    row[flag] = True
            for file_arg in file_args:
                with open(row[file_arg]) as fd:
                    row[file_arg] = fd.readlines()
            commands.append({'frequency': frequency, 'kwargs': row})
        except Exception as err:
            print("Invalid config on line {}: {}".format(line_number, err), file=sys.stderr)
            continue
    return commands


def alarm_handler(_signum, _frame):
    """Raise timeout on SIGALRM"""
    raise TimeoutError('Timeout')


def call_repeatedly(interval, ctx, func, **kwargs):
    """Repeatedly call a given function after a given interval (will drift)"""
    stopped = Event()
    # the first call is in `interval` secs
    def loop():
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        wait_time = interval
        time_taken = 0
        while not stopped.wait(wait_time):
            try:
                if DEBUG >= 1:
                    print("TICK: {}".format(",".join([str(x) for x in kwargs.values()])))
                start_time = time.time()
                signal.signal(signal.SIGALRM, alarm_handler)
                signal.alarm(interval)
                ctx.invoke(func, **kwargs)
                signal.alarm(0)
                time_taken = time.time() - start_time
                #print("TOOK: {}".format(time_taken))
            except Exception as err:
                print("ERROR: {}".format(err), file=sys.stderr)
            finally:
                wait_time = interval - time_taken
    proc = Process(target=loop)
    proc.start()
    def stop():
        stopped.set()
        proc.join(2)
        if proc.is_alive():
            proc.terminate()
            proc.join()
    return stop


def run_click_command_as_a_service(ctx, func, commands, delay=0):
    """Open a sub-process for each command and call it repeatedly based on the
    frequency"""
    # pylint: disable=W0603
    global SERVICE_MODE
    SERVICE_MODE = True

    if not commands:
        raise ValueError("Missing commands")

    cleanup = []
    def term_handler(_signum, _frame):
        for stop in cleanup:
            stop()
    signal.signal(signal.SIGTERM, term_handler)
    signal.signal(signal.SIGINT, term_handler)

    for command in commands:
        stop = call_repeatedly(command['frequency'], ctx, func, **command['kwargs'])
        cleanup.append(stop)
        if delay > 0:
            time.sleep(delay)

    # wait forever
    try:
        signal.pause()
    except KeyboardInterrupt:
        if DEBUG:
            print("Cleaning up...")
    term_handler(None, None)


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
    write_log(log_directory, header_line, data_line, changes_only)


def write_log(log_directory, header_line, data_line, changes_only=False):
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
    # Rotate latest. Doesn't matter if the header format has changed
    try:
        if not os.path.isfile(archive_file):
            write_header = True
            if DEBUG > 1:
                print(" * ARCHIVE doesn't exist, rotating LATEST")
            os.remove(latest_file)
        else:
            # Files exist, so make sure that the header line matches
            if header_has_changed(header_line, latest_file):
                # Move the old file and remove latest.csv this will force the
                # agent to reprocess the header
                os.rename(archive_file, archive_file + ".1")
                if DEBUG:
                    print(" * Configuration change detected, replacing " + LATEST_FILENAME)
                os.remove(latest_file)
                write_header = True
    except FileNotFoundError:
        pass

    if changes_only and not data_has_changed(data_line, latest_file):
        return

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


def check_pidfile(pidfile):
    """Check that a process is not running more than once, using PIDFILE"""
    # Check PID exists and see if the PID is running
    if os.path.isfile(pidfile):
        pidfile_handle = open(pidfile, 'r')
        # try and read the PID file. If no luck, remove it
        try:
            pid = int(pidfile_handle.read())
            pidfile_handle.close()
            if check_pid(pid):
                return True
        except:
            pass

        # PID is not active, remove the PID file
        os.unlink(pidfile)

    # Create a PID file, to ensure this is script is only run once (at a time)
    pid = str(os.getpid())
    open(pidfile, 'w').write(pid)
    return False


def check_pid(pid):
    """This function will check whether a PID is currently running"""
    try:
        # A Kill of 0 is to check if the PID is active. It won't kill the process
        os.kill(pid, 0)
        if DEBUG > 1:
            print("Script has a PIDFILE where the process is still running")
        return True
    except OSError:
        if DEBUG > 1:
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


def run_program(prog_list, shell=False):
    """Run a program and check program return code. Note that some commands
    don't work well with Popen.  So if this function is specifically called
    with 'shell=True', then it will run the old 'os.system'. In which case,
    there is no program output """
    try:
        if shell:
            command = " ".join(prog_list)
            os.system(command)
            return True
        process = Popen(prog_list, stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()
        retcode = process.returncode
        if DEBUG >= 1:
            print("Program : {}".format(" ".join(prog_list)))
            print("Return Code: {}".format(retcode))
            print("Stdout: {}".format(stdout))
            print("Stderr: {}".format(stderr))
        return bool(retcode)
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


def header_has_changed(header_line, latest_file):
    """Check if the file header has changed. If it has, rotate the files"""
    # pylint: disable=W0603
    global SERVICE_MODE, SERVICE_CACHE
    current_header_line = None
    if SERVICE_MODE:
        if latest_file not in SERVICE_CACHE:
            SERVICE_CACHE[latest_file] = {}
        if 'header' in SERVICE_CACHE[latest_file]:
            current_header_line = SERVICE_CACHE[latest_file]['header']
        # update the cache
        SERVICE_CACHE[latest_file]['header'] = header_line

    if current_header_line is None:
        with open(latest_file) as latest:
            current_header_line = latest.readline()

    if header_line != current_header_line:
        if DEBUG >= 2:
            print(" * Header has changed")
            print("OLD HEADER: " + current_header_line.strip())
            print("NEW HEADER: " + header_line.strip())
        return True

    return False


def data_has_changed(data_line, latest_file):
    """Check if the data has changed."""
    # pylint: disable=W0603
    global SERVICE_MODE, SERVICE_CACHE
    last_data_line = None
    if SERVICE_MODE:
        if latest_file not in SERVICE_CACHE:
            SERVICE_CACHE[latest_file] = {}
        if 'last_line' in SERVICE_CACHE[latest_file]:
            last_data_line = SERVICE_CACHE[latest_file]['last_line']
        # update the cache
        SERVICE_CACHE[latest_file]['last_line'] = data_line

    if last_data_line is None:
        try:
            with open(latest_file) as latest:
                lines = tail(latest)
                if lines:
                    last_data_line = lines[0]
        except FileNotFoundError:
            return True

    # Skip over the Datetime field, as it will change constantly
    first_comma = data_line.find(',')
    if data_line[first_comma:] != last_data_line[first_comma:]:
        return True

    if DEBUG or not SERVICE_MODE:
        print("No change since last reading", file=sys.stderr)
    return False


def tail(file_handle, lines=1, _buffer=4098):
    """Tail a file and get X lines from the end"""
    # place holder for the lines found
    lines_found = []

    # block counter will be multiplied by buffer
    # to get the block size from the end
    block_counter = -1

    # loop until we find X lines
    while len(lines_found) < lines:
        try:
            file_handle.seek(block_counter * _buffer, os.SEEK_END)
        except IOError:  # either file is too small, or too many lines requested
            file_handle.seek(0)
            lines_found = file_handle.readlines()
            break

        lines_found = file_handle.readlines()

        # decrement the block counter to get the
        # next X bytes
        block_counter -= 1

    return lines_found[-lines:]
