""" Run ETL scripts from the command line. """


from common.logger import configure_logger
from common.parser import InternalToolsParser
from scripts import all_scripts, run_etl_scripts


def script(arg):
    valid_scripts = list(all_scripts.keys())

    if arg in valid_scripts:
        return all_scripts[arg]
    else:
        raise ValueError


def parse_command_line():
    parser = InternalToolsParser(
        prog='python -m scripts',
        description=__doc__
    )

    parser.add_argument(
        '--script',
        dest='script',
        action='store',
        nargs='*',
        type=script,
        default=list(all_scripts.values())
    )

    return vars(parser.parse_args())


if __name__ == '__main__':
    options = parse_command_line()
    configure_logger()

    scripts = options.pop('script')
    run_etl_scripts(scripts)
