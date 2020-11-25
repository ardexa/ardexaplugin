from multiprocessing import Event, Process
import signal
from subprocess import Popen, run, PIPE
import sys
import time
import traceback

def alarm_handler(_signum, _frame):
    """Raise timeout on SIGALRM"""
    raise RuntimeError('Timeout')
    raise TimeoutError('Timeout')


def use_popen():
    process = Popen(['sh', '-c', 'sleep 5; echo 1'], stdout=PIPE, stderr=PIPE)
    print('dmm 1')
    stdout, stderr = process.communicate()
    print('dmm 2')
    print(stdout)
    print(stderr)


def use_run():
    process = run(['sh', '-c', 'sleep 5; echo 1'], stdout=PIPE, stderr=PIPE)
    print(process.stdout.decode())
    print(process.stderr.decode())


def loop():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    for i in range(5):
        try:
            print("TICK: {}".format(i))
            start_time = time.time()
            signal.signal(signal.SIGALRM, alarm_handler)
            signal.alarm(2)
            use_run()
            #use_popen()
            signal.alarm(0)
            time_taken = time.time() - start_time
            print("TOOK: {}".format(time_taken))
        except Exception as err:
            signal.alarm(0)
            time_taken = time.time() - start_time
            print("ERROR: {}".format(err), file=sys.stderr)
            if not isinstance(err, TimeoutError):
                traceback.print_tb(err.__traceback__, file=sys.stderr)
        time.sleep(2)

stopped = Event()
proc = Process(target=loop)
proc.start()
try:
    signal.pause()
except KeyboardInterrupt:
    stopped.set()
    proc.join(2)
    if proc.is_alive():
        proc.terminate()
        proc.join()
