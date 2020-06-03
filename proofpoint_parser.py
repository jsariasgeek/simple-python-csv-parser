import dateutil.parser
import logging
import pandas as pd
from io import StringIO

DEFAULT_SUPPORTED_FORMATS = ['%m/%d/%Y %H:%M']


class TimeParseException(Exception):
    """
        Could not parse the date format given
        with the format strings provided
    """
    pass


def from_date(source_column_name, formats=DEFAULT_SUPPORTED_FORMATS):
    """
    :param source_column_name:
    :param formats:
    :return: Parsed Data for Datasaurs
    """

    # Return a function that will parse the column
    def from_date_inner(df):
        date_string = df.get(source_column_name)
        if date_string == None:
            return
        # Try parsing with formats provided.
        for frmt in formats:
            try:
                # Make a test of bad formats for the date passed in
                date_time = datetime.strptime(date_string, frmt)
                return date_time.strftime("%Y-%m-%d %H:%M")
            except Exception:
                continue

        # Try dateutil will try iso 8601 and other timezone aware formats.
        try:
            # This code is problematic because it will ignore timezone in the output.
            # We need to make this all time zone aware, means getting consistent timezones
            # from clients for a feed.
            date_time = dateutil.parser.parse(date_string)
            return date_time.strftime("%Y-%m-%d %H:%M")
        except Exception:
            logging.info(f"Unable to parse date for {source_column_name} falling back to provided formats.")
        raise TimeParseException()

    return from_date_inner


def direct_mapping(column_name):
    return lambda x: x.get(column_name)


class ProofPointParser:
    """
    ProofPointParser
    """
    def __init__(self, file):
        self.file = file

    """
    This class enables us to parse the csv data from ProofPoint Vendor
    """
    needed_columns = [
        '_time',  # when time
        'recipient',
        'clickTime'  # what field
    ]

    mappings = {
        'email': direct_mapping('recipient'),
        'sent_date': from_date('_time'),
        'clicked_date': from_date('clickTime')
    }

    def parse(self):
        """
        :return: PD DataFrame
        """
        df = pd.read_csv(self.file)
        target_df = pd.DataFrame()

        # Drop rows that contain all empty values.
        df = df.dropna(axis=0, how='all')
        # Replace null values with None
        df = df.where((pd.notnull(df)), None)
        # Limit df to just the columns we need
        df = df[self.needed_columns]

        for field, field_lambda in self.mappings.items():
            target_df[field] = df.apply(field_lambda, axis=1)
        return target_df

    def get_csv(self):
        target_df = self.parse()
        csv_buffer = StringIO()
        target_df.to_csv(csv_buffer, index=False)
        return csv_buffer




