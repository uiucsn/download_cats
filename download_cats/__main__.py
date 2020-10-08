import argparse

from download_cats import FETCHERS
from download_cats.utils import configure_logging


def parse_args():
    parser = argparse.ArgumentParser('Download astronomical catalogues')
    parser.add_argument('-d', '--dir', default='.', help='destination directory')
    parser.add_argument('-j', '--jobs', default=1, type=int, help='number of parallel job to run')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='logging verbosity')

    subparsers = parser.add_subparsers(
        dest='catalog',
        title='catalogs',
        description='available catalogs',
        help='each catalog can have its specific options, see {catalog} --help',
        required=True,
    )
    for name, fetcher in FETCHERS.items():
        fetcher_parser = subparsers.add_parser(name)
        fetcher.add_arguments_to_parser(fetcher_parser)

    args = parser.parse_args()
    return args


def main():
    cli_args = parse_args()
    configure_logging(cli_args)
    fetcher = FETCHERS[cli_args.catalog](cli_args)
    fetcher()


if __name__ == "__main__":
    main()
