from argparse import ArgumentParser, Namespace
import logging

from put_cat_to_ch import ARG_SUB_PARSERS, ZtfPutter
from put_cat_to_ch.ztf import CURRENT_ZTF_DR


def parse_clickhouse_settings(s):
    settings = dict(map(str.strip, pair.split('=')) for pair in s.split(','))
    return settings


def parse_args() -> Namespace:
    parser = ArgumentParser('Put astronomical catalogue to ClickHouse')
    parser.add_argument('-d', '--dir', default='.', help='directory containing data files')
    parser.add_argument('--csv-dir', default=None,
                        help='directory to store temporary CSV files, default is <DIR>/csv')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='logging verbosity')
    parser.add_argument('-u', '--user', default='default', help='ClickHouse username')
    parser.add_argument('--host', default='localhost',
                        help='Clickhouse hostname, may include port number with semicolon')
    parser.add_argument('-c', '--clickhouse-settings', default={}, type=parse_clickhouse_settings,
                        help='additional settings for clickhouse server, format as "key1=value1,key2=value2"')

    subparsers = parser.add_subparsers(
        dest='catalog',
        title='catalogs',
        description='subcommand to put some catalog into ClickHouse database',
        help='each catalog has its specific options, see {catalog} --help',
        required=True,
    )
    for command, catalog_parser in ARG_SUB_PARSERS.items():
        sub_parser = subparsers.add_parser(command)
        catalog_parser.add_arguments_to_parser(sub_parser)

    args = parser.parse_args()
    return args


def configure_logging(cli_args):
    if cli_args.verbose == 0:
        logging_level = logging.ERROR
    elif cli_args.verbose == 1:
        logging_level = logging.WARNING
    elif cli_args.verbose == 2:
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG
    logging.basicConfig(level=logging_level)


def main():
    cli_args = parse_args()
    configure_logging(cli_args)
    logging.info(cli_args)

    parser = ARG_SUB_PARSERS[cli_args.catalog](cli_args)
    parser()


if __name__ == '__main__':
    main()
