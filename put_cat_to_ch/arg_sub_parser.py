from argparse import ArgumentParser, Namespace

from put_cat_to_ch.putter import PutterMeta


__all__ = ('ArgSubParser',)


def _action_type(s):
    s = s.lower()
    s = s.replace('-', '_')
    return s


class ArgSubParser:
    command: str
    putter_cls: PutterMeta

    def __init_subclass__(cls, **kwargs):
        if cls.command is None or cls.putter_cls is None:
            raise NotImplementedError
        super().__init_subclass__(**kwargs)

    def __init__(self, cli_args: Namespace):
        self.cli_args = cli_args
        self.enabled_actions = self.cli_args.action
        self.putter = self.putter_cls(**vars(self.cli_args))

    def __call__(self):
        """Call putter"""
        self.putter(self.cli_args.action)

    @classmethod
    def add_arguments_to_parser(cls, parser: ArgumentParser):
        """Add fetcher-specific arguments to argument parser"""
        parser.add_argument('-a', '--action', type=_action_type, nargs='+',
                            default=cls.putter_cls.default_actions, choices=cls.putter_cls.available_actions,
                            help='actions to perform')
