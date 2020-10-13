from argparse import ArgumentParser
import logging

from put_cat_to_ch import ZtfPutter


def parse_args():
    parser = ArgumentParser('Put astronomical catalogue to ClickHouse')
    parser.add_argument('-d', '--dir', default='.', help='directory containing data files')
    parser.add_argument('-j', '--jobs', default=1, type=int, help='number of parallel job to run')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='logging verbosity')
    parser.add_argument('-u', '--user', default='default', help='ClickHouse username')
    parser.add_argument('--host', default='localhost',
                        help='Clickhouse hostname, may include port number with semicolon')
    parser.add_argument('-e', '--on_exists', default='fail', type=str.lower, choices={'fail', 'keep', 'drop'},
                        help='what to do when some of tables to create already exists, "fail" terminates the program, '
                             '"keep" does nothing, and "drop" recreates the table')
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

    putter = ZtfPutter(**vars(cli_args))
    putter()


if __name__ == '__main__':
    main()
