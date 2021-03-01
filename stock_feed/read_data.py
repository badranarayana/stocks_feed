import csv
from datetime import datetime


# helper functions to convert to python objects
def parse_time(str_time):
    return datetime.strptime(str_time, '%H:%M:%S').time()


def parse_date(str_date):
    return datetime.fromisoformat (str_date)

# can be re ued in entire module
TRADING_START_TIME = parse_time('09:30:00')
TRADING_END_TIME = parse_time('16:30:00')


class BaseFeedData(object):
    """
    This class is responsible to load/feed the data
    from different data sources(.csv, .txt, databases)
    """

    def read_data(self, path):
        # assuming field names are constant
        with open(path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',', fieldnames=['date', 'time', 'symbol', 'price'])
            for row in csv_reader:
                if not self.is_invalid_quote(quote_time=row['time']):
                    # only process valid quotes
                    row['date'] = parse_date(row['date'])
                    row['time'] = parse_time(row['time'])
                    yield row

    @staticmethod
    def is_invalid_quote(quote_time):
        if not quote_time:
            raise ValueError("quote_time is required field")

        # check quote time fall out of market hours
        quote_time = parse_time(quote_time)
        if quote_time < TRADING_START_TIME or quote_time > TRADING_END_TIME:
            # Invalid quote
            return True
        return False


class Calculations(object):

    def __init__(self, trading_day, day_quotes):
        self.trading_day = trading_day
        self._quotes = day_quotes

    def valid_quotes_per_day(self):
        return len(self._quotes)

    def last_quote_time(self):
        # assuming last item in quotes
        return self._quotes[-1]['time']

    def most_active_hour(self):
        # store quotes count per hour
        maximum_quotes = {}
        for row in self._quotes:
            hour = row['time'].hour
            if hour not in maximum_quotes:
                maximum_quotes[hour] = 1
            elif hour in maximum_quotes:
                maximum_quotes[hour] += 1

        values = list(maximum_quotes.values())
        max_value = max(values)
        if values.count(max_value) > 1:
            for hour, quote_count in maximum_quotes.items():
                if quote_count == max_value:
                    return hour
        else:
            swap_dict = {val: key for key, val in maximum_quotes.items()}
            return swap_dict[max_value]

    def most_active_symbol(self):
        # store quotes count per hour
        active_quotes = {}
        for row in self._quotes:
            symbol = row['symbol']
            if symbol not in active_quotes:
                active_quotes[symbol] = 1
            elif symbol in active_quotes:
                active_quotes[symbol] += 1

        values = list(active_quotes.values())
        max_value = max(values)
        if values.count(max_value) > 1:
            for symbol, quote_count in active_quotes.items():
                if quote_count == max_value:
                    return symbol
        else:
            swap_dict = {val: key for key, val in active_quotes.items()}
            return swap_dict[max_value]  # return stock symbol

    def process_data(self):
        print( f"Trading Day= {self.trading_day} ")
        print( f"Last Quote Time = {self.last_quote_time()}")
        print(f"Number of valid quotes = {self.valid_quotes_per_day()}")
        print(f"Most active hour = {self.most_active_hour()}")
        print(f"Most active symbol = {self.most_active_symbol()}")


class PipeLine(BaseFeedData):
    _cls_calculation = Calculations
    def __init__(self, file_path):

        if not file_path:
            raise ValueError("No file found")
        self.data = self.read_data(file_path)

    def run(self):
        day = None
        day_quotes = []
        for row in self.data:
            if day is None:
                day = row['date']
                day_quotes.append(row)
            elif day == row['date']:
                day_quotes.append(row)
            elif day != row['date']:
                # calculate the previous day and process next day data
                self._cls_calculation(trading_day=day, day_quotes=day_quotes).process_data()
                print(20 * "**")
                day = row['date']
                day_quotes.clear()  # clearing processed quotes
                day_quotes.append(row)  # adding next day quote
            else:
                day_quotes.clear()
        else:
            if day_quotes:
                self._cls_calculation(trading_day=day, day_quotes=day_quotes).process_data()


if __name__ == '__main__':
    PipeLine(file_path="stock_data.csv").run()