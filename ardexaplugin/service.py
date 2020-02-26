"""Service mode"""

import csv
import signal
import sys
import time
from multiprocessing import Event, Process
from .dynamic import activate_service_mode

DEBUG = 0

def set_debug(debug):
    """Set the debug level across the whole module"""
    # pylint: disable=W0603
    global DEBUG
    DEBUG = debug


def parse_service_file(command_file, file_args=[]):
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
                with open(row[file_arg]) as file_handle:
                    row[file_arg] = file_handle.readlines()
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
    activate_service_mode()

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
