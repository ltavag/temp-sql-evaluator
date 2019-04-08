OP_MAPPINGS = {
    "=": "=="
}

class TableFilter:
    """Class that allows you to create a function given
    certain filter conditions. 

    * This class implements __call__ so it can be used as a
    function once initialized. 
    * It only evaluates one row at a time so it can be pipelined
    with itertools.filter.
    """
    def __init__(self, conditions):
        self.conditions = []

        for condition in conditions:
            self.conditions.append('{left} {op} {right}'.format(
                left = self.expression(condition['left']),
                right = self.expression(condition['right']),
                op = OP_MAPPINGS.get(condition['op'], condition['op'])
            ))

    def expression(self, e):
        if 'literal' in e:
            if isinstance(e['literal'], str):
                return "'{}'".format(e['literal'])

            return "{}".format(e['literal'])

        if 'column' in e:
            return 'row["{}.{}"]'.format(
                    e['column']['table'],
                    e['column']['name']
            )


    def __call__(self, row):
        for condition in self.conditions:
            if not eval(condition):
                return False

        return True
