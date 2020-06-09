import dateutil.parser
import logging
import pytz
import pandas as pd
from io import StringIO

DEFAULT_SUPPORTED_FORMATS = ['%m/%d/%Y %H:%M']


class TimeParseException(Exception):
    """
        Could not parse the date format given
        with the format strings provided
    """
    pass


def from_date_timezone_aware(column_name):
    """
    :param column_name:
    :return:
    """
    def from_date_timezone_aware_inner(df):
        """
        :param df: Dataframe. The date column should be in iso8601 format
        2020-04-27T00:32:35.120-0700
        2020-04-27T06:43:32.000Z
        :return:
        """
        date_string = df.get(column_name)
        if date_string == None:
            return
        date_time_obj = dateutil.parser.parse(date_string)
        date_time_obj_in_utc = date_time_obj.astimezone(pytz.utc)
        return date_time_obj_in_utc.strftime("%Y-%m-%d %H:%M")
    return from_date_timezone_aware_inner


def split_row_into_multiple_records(df):
    """
    :param df:
    :return: If one row in the dataframe has different emails for recipient, ccAddresses and toAddresses
    we need to create one row per email
    """
    new_df = pd.DataFrame()

    def create_new_row(row_to_split, recipient_value):
        new_row = dict(row_to_split)
        new_row['recipient'] = recipient_value
        new_row['ccAddresses{}'] = None # create other fields as None
        new_row['toAddresses{}'] = None # create other fields as None
        return new_row

    for index, row in df.iterrows():
        # append the row into the new df
        new_df = new_df.append(dict(row), ignore_index=True)
        recipient = row['recipient']
        cc_address = row['ccAddresses{}']
        to_address = row['toAddresses{}']

        if cc_address and recipient and cc_address != recipient:
            # add another row. there is two different emails
            new_row = create_new_row(row, cc_address)
            new_df = new_df.append(new_row, ignore_index=True)

        if to_address and cc_address and to_address != cc_address:
        # add another row. there is two different emails
            new_row = create_new_row(row, to_address)
            new_df = new_df.append(new_row, ignore_index=True)

    return new_df


def mapping_to_multiple_fields(column_list):
    """
    :param args: Column names
    :return: If look for a value in every column name in the list. If
    the value is not None it returns that value
    """
    def mapping_to_multiple_fields_inner(df):
        for column in column_list:
            value = df.get(column)
            if value is not None:
                return value
    return mapping_to_multiple_fields_inner


class ProofPointParser:
    """
    ProofPointParser
    """
    def __init__(self, file):
        self.file = file

    """
    This class enables us to parse the csv data from ProofPoint Vendor
    """

    EMAIL_FIELDS = ['recipient', 'ccAddresses{}', 'toAddresses{}']

    needed_columns = [
        '_time',  # when time
        'recipient',
        'clickTime',  # what field
        'messageTime',
        'ccAddresses{}',
        'toAddresses{}'
    ]

    mappings = {
        'email': mapping_to_multiple_fields(EMAIL_FIELDS),
        'sent_date': from_date_timezone_aware('_time'),
        'clicked_date': from_date_timezone_aware('messageTime')
    }

    def parse(self):
        """
        :return: PD DataFrame
        """
        df = pd.read_csv(self.file)
        target_df = pd.DataFrame()

        # Drop rows that contain all empty values.
        df = df.dropna(axis=0, how='all')

        # Drop rows without email fields
        df = df.dropna(axis=0, how='all', subset=self.EMAIL_FIELDS)

        # Replace null values with None
        df = df.where((pd.notnull(df)), None)

        # filter by real word phising behaviors
        df = df[(df['eventType']== 'messagesBlocked') | (df['eventType'] == 'clicksBlocked') | (df['eventType'] == 'MessagesDelivered')]

        # Limit df to just the columns we need
        df = df[self.needed_columns]

        # create additional records if there is different email addresses
        df = split_row_into_multiple_records(df)

        for field, field_lambda in self.mappings.items():
            target_df[field] = df.apply(field_lambda, axis=1)
        return target_df

    def get_csv(self):
        target_df = self.parse()
        csv_buffer = StringIO()
        target_df.to_csv(csv_buffer, index=False)
        return csv_buffer




