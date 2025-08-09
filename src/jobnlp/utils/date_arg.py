import argparse
from datetime import datetime, date


class DateFormatError(Exception):
    def __init__(self, err_msg: str | None = None):
        if err_msg:
            super().__init__(err_msg)
        else:
            super().__init__("Invalid date format. Expected YYYY-MM-DD.")


def parse_date_arg() -> date:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        help="Execution date in format: YYYY-MM-DD (default: today)",
    )
    args = parser.parse_args()

    if args.date:
        try:
            return datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError as e:
            raise DateFormatError(
                "Invalid date format. Use YYYY-MM-DD.") from e
    else:
        return date.today()

def get_exec_date(log, caller_name: str = "") -> date:
    """
    Parses date argument and logs execution context.
    Returns a datetime.date object.
    """
    try:
        exec_date = parse_date_arg()
        exec_date_f = exec_date.strftime("%Y-%m-%d")
        log.info((f"Running {caller_name or '__main__'}"
                 f"for date: {exec_date_f}"))
        return exec_date
    except DateFormatError as e:
        log.error(e)
        exit(1)