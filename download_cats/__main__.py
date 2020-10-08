import argparse
import logging


from download_cats import FETCHERS


def parse_args():
    parser = argparse.ArgumentParser('Download astronomical catalogues')
    parser.add_argument('catalog', metavar='CAT', type=str.lower,
                        help=f'catalog name, supported catalogs are: {", ".join(FETCHERS)}')
    parser.add_argument('-d', '--dir', default='.', help='destination directory')
    parser.add_argument('-j', '--jobs', default=1, type=int, help='number of parallel job to run')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='logging verbosity')
    args = parser.parse_args()
    return args


def configure_logging(args):
    if args.verbose == 0:
        logging_level = logging.ERROR
    elif args.verbose == 1:
        logging_level = logging.WARNING
    elif args.verbose == 2:
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG
    logging.basicConfig(level=logging_level)


def main():
    args = parse_args()
    configure_logging(args)
    FETCHERS[args.catalog](args)


if __name__ == "__main__":
    main()
