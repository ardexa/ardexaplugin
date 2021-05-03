"""
common functions for reading/writing checkpoint files (datetime only)
"""

import os
from dateutil.parser import isoparse
from .dynamic import datetime_to_str

def read_checkpoint(checkpoint_file):
    """Read the checkpoint file"""
    os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
    timestamp = None
    try:
        with open(checkpoint_file) as cp_file:
            timestamp = isoparse(cp_file.readline().strip())
    except FileNotFoundError:
        pass
    except ValueError:
        pass
    return timestamp


def write_checkpoint(checkpoint_file, timestamp):
    """Write the checkpoint file"""
    if timestamp is None:
        return
    with open(checkpoint_file, 'w') as cp_file:
        cp_file.write(datetime_to_str(timestamp))
