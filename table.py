import json
from table_filter import TableFilter

DATA_TYPES = {
    "int": int,
    "str": str,
}

class Table(list):
    """Class that inherits from a list and represents an in memory table.

    * Rows are loaded from a hard coded path to the table.json file
    * Rows are coalesced to proper data types on import.
    * Each row is represented by a dictionary so we can easily access keys.
    """

    def __init__(self, table, name):
        self.name = name
        self.table = table
        table = json.load(open(f'examples/{ table }.table.json'))

        headers, *rows = table
        self.headers = {h[0]: h[1] for h in headers}

        for row in rows:
            coalesced_row = {}
            for index, field in enumerate(row):
                field_name, field_type = headers[index]
                aliased_name = '.'.join([self.name, field_name])
                coalesced_row[aliased_name] = DATA_TYPES[field_type](field)

            self.append(coalesced_row) 

    def filter(self, conditions):
        return filter(TableFilter(conditions), self)
