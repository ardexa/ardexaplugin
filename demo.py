"""Mock plugin
  Usage: python demo.py -vv log 1.2.3.4 /tmp/ardexa/logs
"""

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


def get_data(_ip_address):
    """Do the thing, get the data"""
    readings = [1, 3.5, True, "ok"]
    return list(zip(HEADER, readings))


@click.group()
@click.option('-v', '--verbose', count=True)
def cli(verbose):
    """Command line entry point"""
    global DEBUG
    DEBUG = verbose
    ap.set_debug(verbose)


@cli.command()
@click.argument('ip_address')
@click.argument('output_directory')
@click.option('-c', '--changes-only', is_flag=True)
def log(ip_address, output_directory, changes_only):
    """Fetch and log data"""
    data = get_data(ip_address)

    # table and source
    table = "table"
    source = [ip_address, 502]

    # print(list(data))
    ap.write_dyn_log(output_directory, table, source, data, changes_only)


if __name__ == "__main__":
    cli()
