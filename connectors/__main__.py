""" Test data connections: authenticate or send a test query. """


from common.logger import configure_logger
from common.parser import InternalToolsParser
from connectors import all_connectors, test_connections


def connector(arg):
    valid_connectors = list(all_connectors.keys())

    if arg in valid_connectors:
        return all_connectors[arg]
    else:
        raise ValueError


def parse_command_line():
    parser = InternalToolsParser(
        prog='python -m connectors',
        description=__doc__
    )

    parser.add_argument(
        '--test',
        dest='test',
        action='store',
        choices=['login', 'extract'],
        default='login'
    )

    parser.add_argument(
        '--connector',
        dest='connector',
        action='store',
        nargs='*',
        type=connector,
        default=list(all_connectors.values())
    )

    return vars(parser.parse_args())


if __name__ == '__main__':
    options = parse_command_line()
    configure_logger()

    connectors = options.pop('connector')
    test = options.pop('test')

    test_connections(connectors, test, **options)
