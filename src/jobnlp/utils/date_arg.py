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
