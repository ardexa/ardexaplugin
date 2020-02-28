#!/usr/bin/python
"""Mock plugin
  Usage: python demo.py -vv log 1.2.3.4 /tmp/ardexa/logs
"""

import time
import click
import ardexaplugin as ap

DEBUG = 0
HEADER = [{
    "name": "counter",
    "type": "integer",
    "units": "kWh"
}, {
    "name": "cosphi",
    "type": "decimal",
}, {
    "name": "gate open",
    "type": "bool",
}, {
    "name": "status",
    "type": "keyword",
}]

DATA = [
    [1, 3.5, True, "ok"],
    [1, 3.5, True, "ok"],
    [2, 3.5, True, "ok"],
    [1, 3.5, True, "ok"],
]

def get_data(ip_address, call):
    """Do the thing, get the data"""
    if ip_address == '0.0.0.0':
        time.sleep(10)
    if ip_address == '1.2.3.5':
        time.sleep(1)
    readings = DATA[call]
    return list(zip(HEADER, readings))


@click.group()
@click.option('-v', '--verbose', count=True)
def cli(verbose):
    """Command line entry point"""
    # pylint: disable=W0603
    global DEBUG
    DEBUG = verbose
    ap.set_debug(verbose)


@cli.command()
@click.argument('ip_address')
@click.argument('output_directory')
@click.option('-c', '--changes-only', is_flag=True)
def pid(ip_address, output_directory, changes_only):
    """Fetch and log data"""
    # table and source
    table = "table"
    source = [ip_address, 502]

    try:
        ap.check_pidfile("modbus", source)
    except ap.PluginAlreadyRunning:
        print("Already running")
        return

    data = get_data(ip_address, 0)
    ap.write_dyn_log(output_directory, table, source, data, changes_only)


@cli.command()
@click.argument('ip_address')
@click.argument('output_directory')
@click.option('-s', '--source-map', type=click.File('r'))
@click.option('-c', '--changes-only', is_flag=True)
def log(ip_address, output_directory, source_map, changes_only):
    """Fetch and log data"""
    # table and source
    table = "table"
    source = [ip_address, 502]

    ap.load_source_map(source_map)

    data = get_data(ip_address, 0)
    meta = (("Example", "ttyUSB"), ("Port", 502))
    ap.print_verbose_log(table, source, meta, data)

    print('---')
    source = ["inverter", "/dev/ttyS0", 1]
    ap.print_verbose_log(table, source, (), data)
    #ap.write_dyn_log(output_directory, table, source, data, changes_only)


@cli.command()
@click.argument('ip_address')
@click.argument('config_file', type=click.File('r'))
@click.argument('output_directory')
@click.option('--port', default=8080)
@click.option('-c', '--changes-only', is_flag=True)
def config(ip_address, config_file, output_directory, port, changes_only):
    """Fetch and log data"""
    # table and source
    table = "table"
    source = [ip_address, 502]

    try:
        ap.check_pidfile("modbus", source)
    except ap.PluginAlreadyRunning:
        print("Already running")
        return

    data = get_data(ip_address, 0)
    meta = (("Example", "ttyUSB"), ("Port", port))
    ap.print_verbose_log(table, source, meta, data)
    #ap.write_dyn_log(output_directory, table, source, data, changes_only)
    #ap.remove_pidfile("modbus", source)


@cli.command()
@click.argument('ip_address')
@click.argument('output_directory')
@click.option('-c', '--changes-only', is_flag=True)
def log4(ip_address, output_directory, changes_only):
    """Fetch and log data"""
    # table and source
    table = "table"
    source = [ip_address, 502]

    for call in range(len(DATA)):
        data = get_data(ip_address, call)
        # print(list(data))
        ap.write_dyn_log(output_directory, table, source, data, changes_only)
        time.sleep(1)


@cli.command()
@click.argument('ip_address')
@click.argument('output_directory')
@click.option('-c', '--changes-only', is_flag=True)
def change(ip_address, output_directory, changes_only):
    """Fetch and log data"""
    # table and source
    table = "table"
    source = [ip_address, 502]

    data = get_data(ip_address, 0)
    data.append(('junk(discard)', 'ignore'))
    ap.write_dyn_log(output_directory, table, source, data, changes_only)


@cli.command()
@click.argument('command_file', type=click.File('r'))
@click.pass_context
def service(ctx, command_file):
    commands = ap.parse_service_file(command_file, ['config_file'])
    ap.run_click_command_as_a_service(ctx, config, commands)


@cli.command()
@click.argument('command_file', type=click.Path(exists=True))
def activate_service(command_file):
    ap.activate_service('demo', 'demo_ardexa', command_file)


if __name__ == "__main__":
    cli()
