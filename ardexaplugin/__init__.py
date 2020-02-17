"""
ardexaplugin
============

:copyright: (c) 2020 Ardexa Pty Limited
:license: MIT, see LICENSE for more details.
"""

import struct
import time
import os
from subprocess import Popen, PIPE
from .dynamic import (
    write_dyn_log,
    set_debug as dyn_set_debug,
)
from .service import (
    parse_service_file,
    run_click_command_as_a_service,
    set_debug as svc_set_debug,
)

DEBUG = 0

def set_debug(debug):
    """Set the debug level across the whole module"""
    # pylint: disable=W0603
    global DEBUG
    DEBUG = debug
    dyn_set_debug(debug)
    svc_set_debug(debug)


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
