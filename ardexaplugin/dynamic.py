"""Dynamic mapping"""

import csv
import os
import sys
import time
from io import StringIO

DEBUG = 0
LATEST_FILENAME = "latest.csv"
SERVICE_MODE = False
SERVICE_CACHE = {}

def set_debug(debug):
    """Set the debug level across the whole module"""
    # pylint: disable=W0603
    global DEBUG
    DEBUG = debug


def activate_service_mode():
    """Run in service mode"""
    # pylint: disable=W0603
    global SERVICE_MODE
    SERVICE_MODE = True


def get_datetime_str():
    """This function gets the local time with local timezone offset"""
    return time.strftime('%Y-%m-%dT%H:%M:%S%z')


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


def clean_source_name(source):
    """Clean up the source name"""
    # Make sure it doesn't start with a slash
    if source.startswith('/dev'):
        source = source[4:]
    while source.startswith('/'):
        source = source[1:]
    return source


def get_source_name(source):
    """Convert the source to a directory"""
    if isinstance(source, str):
        return clean_source_name(source)
    if not isinstance(source, list):
        raise ValueError("Unknown source format")
    return os.path.join(*[clean_source_name(str(s)) for s in source])


def get_log_directory(output_directory, table, source):
    """Generate final log directory.

    Based on the table(string) and source(list). Create the target directory
    if it doesn't exist"""
    log_directory = os.path.join(output_directory, table, get_source_name(source))
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


def print_verbose_log(table, source, meta, data):
    """Pretty print the log output to screen. Append Datetime"""
    output = (("Table", table), ("Source", get_source_name(source)), *meta, *data)
    output = [(process_header_field(item[0]), clean_and_stringify_value(item[1]))
              for item in output]
    max_key_len = max(len(item[0]) for item in output)
    max_val_len = max(len(item[1]) for item in output)
    format_str = "  {{:<{}}}  {{:<{}}}".format(max_key_len, max_val_len)
    for item in output:
        print(format_str.format(*item))


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
