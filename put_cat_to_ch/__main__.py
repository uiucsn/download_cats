from argparse import ArgumentParser
import logging

from put_cat_to_ch import ZtfPutter
from put_cat_to_ch.ztf import CURRENT_ZTF_DR


def parse_clickhouse_settings(s):
    settings = dict(map(str.strip, pair.split('=')) for pair in s.split(','))
    return settings


def parse_args():
    parser = ArgumentParser('Put astronomical catalogue to ClickHouse')
    parser.add_argument('-d', '--dir', default='.', help='directory containing data files')
    parser.add_argument('--csv-dir', default=None,
                        help='directory to store temporary CSV files, default is <DIR>/csv')
    parser.add_argument('--dr', default=CURRENT_ZTF_DR, type=int, help='ZTF DR number')
    parser.add_argument('-j', '--jobs', default=1, type=int, help='number of parallel job to run')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='logging verbosity')
    parser.add_argument('-u', '--user', default='default', help='ClickHouse username')
    parser.add_argument('--host', default='localhost',
                        help='Clickhouse hostname, may include port number with semicolon')
    parser.add_argument('-e', '--on_exists', default='fail', type=str.lower, choices={'fail', 'keep', 'drop'},
                        help='what to do when some of tables to create already exists, "fail" terminates the program, '
                             '"keep" does nothing, and "drop" recreates the table')
    parser.add_argument('-a', '--action',
                        default={'gen-csv', 'obs', 'rm-csv', 'meta', 'circle', 'xmatch', 'source-obs', 'source-meta'},
                        choices={'gen-csv', 'obs', 'rm-csv', 'meta', 'circle', 'xmatch', 'source-obs', 'source-meta'},
                        type=str.lower, nargs='+',
                        help='actions to perform, "insert_obs" creates and fill observation table, "insert_meta" does '
                             'the same for meta table assuming that "insert_obs" was performed earlier')
    parser.add_argument('-r', '--radius', default=0.2, type=float, help='cross-match radius, arcsec')
    parser.add_argument('--circle-match-insert-parts', default=1, type=int,
                        help='specifies the number of parts to split meta table to perform insert into circle-match '
                             'table, less parts require less time, but more memory')
    parser.add_argument('--source-obs-insert-parts', default=1, type=int,
                        help='same as --circle-match-insert-parts but for source-obs table')
    parser.add_argument('-c', '--clickhouse-settings', default={}, type=parse_clickhouse_settings,
                        help='additional settings for clickhouse server, format as "key1=value1,key2=value2"')
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

    putter = ZtfPutter(**vars(cli_args))
    putter(cli_args.action)


if __name__ == '__main__':
    main()
