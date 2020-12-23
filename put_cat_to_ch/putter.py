import logging
from inspect import isfunction
from itertools import chain
from typing import Iterable


def _putter_call(self, actions: Iterable[str]):
    for action in actions:
        self.available_actions[action](self)


class PutterMeta(type):
    def __new__(cls, clsname, bases, attrs):
        """Metaclass to use in *Putter classes

        It generates 'available_actions' dict attribute containing action_*
        methods
        """

        actions = {}
        for name, attr in attrs.items():
            if not name.startswith('action_'):
                continue
            if not isfunction(attr):
                logging.warning(f'class {clsname} has {name} attribute which is not a method')
                continue
            _, name = name.split('_', maxsplit=1)
            actions[name] = attr

        default_actions = attrs.setdefault('default_actions', ())

        missed_actions = set(default_actions) - set(actions)
        if missed_actions:
            raise NotImplementedError(f'default_actions contains actions not presented as methods: {missed_actions}')

        # Reorder actions to put default_actions first
        attrs['available_actions'] = {name: actions[name]
                                      for name in chain(default_actions, set(actions) - set(default_actions))}

        attrs['__call__'] = _putter_call

        return super().__new__(cls, clsname, bases, attrs)


class Putter(metaclass=PutterMeta):
    pass


class __ExamplePutter(Putter):
    """Example putter"""
    default_actions = ('x', 'a')

    def action_a(self):
        pass

    def action_b(self):
        pass

    def action_x(self):
        pass
