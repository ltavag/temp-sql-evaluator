import itertools
import json
from collections import defaultdict
from table import Table
from table_filter import TableFilter


def get_table_for_field(field_name, tables):
    """
    Determine the table for a given column name without
    a specificed table. When inferring the table, it's
    important that there be only one possibility. 
    """
    tables_with_field = [
            t
            for t in tables
            if field_name in t.headers
    ]

    if len(tables_with_field) > 1:
        raise Exception('Column "{fieldname}" is ambiguous,'
                        ' because it is in multiple tables: {tables}'.format(
                             fieldname=field_name,
                             tables=[t.name for t in tables_with_field]))

    return tables_with_field[0]

def validate_and_coalesce_select(query, tables):
    """
    1. Check to see that all fieldnames referenced in the select
    have only one source table. 

    If a source table must be infered, it is infered here and 
    then explicitly set in the query object.

    2. Check to see that tables referenced in the SELECT clause
    exist in the FROM clause.

    3. Confirm that the referenced column belongs to the source 
    table specified by the query.
    """
    query_headers = []

    for field in query['select']:

        if not field['column']['table']:
            field['column']['table'] = get_table_for_field(field['column']['name'], tables).name

        if field['column']['table']:
            table_name = field['column']['table']
            source_tables = {x['as']: x['source'] for x in query['from']}

            if table_name not in source_tables:
                raise Exception('Table "{table}" referenced, but not in FROM clause'\
                                .format(table=field['column']['table']))

            table = (t for t in tables if t.name == table_name).__next__()
            if field['column']['name'] not in table.headers:
                raise Exception('Field "{field}" referenced, but not in ' + \
                                'source table {table}'.format(field=field['column']['name'],
                                                             table=source_table_name))
            query_headers.append([
                field['as'],
                table.headers[field['column']['name']]])

    return query_headers


def parse_where_clause(query, tables):
    """
    1. Check to see that all fieldnames referenced in the where 
    have only one source table. 

    If a source table must be infered, it is infered here and 
    then explicitly set in the query object.

    2. Check to see if there is a type mismatch between the LHS
    and RHS of the condition.

    3. Check to see if any conditions can be evaluated before a 
    join.
    """
    early_clauses = defaultdict(list) # Clauses that can be evaluated before a join, namespaced by table name
    late_clauses = [] # Clauses that must be evaluated post join.

    for condition in query['where']:
        condition_types = set() # To check for type mismatches
        tables_involved = set() # To check whether we can evaluate these pre-join.

        for side in ('left', 'right'):
            expression = condition[side]

            if 'column' in expression:
                field_name = expression['column']['name']
                if expression['column']['table']:
                    table = [t for t in tables
                             if t.name == expression['column']['table']][0]

                else:
                    table = get_table_for_field(field_name, tables)
                    expression['column']['table'] = table.name

                """
                    * Take note of the type of the evaluated expression
                    * Keep a counter of how many tables are required to evaluate this expression.
                """
                condition_types.add(table.headers[field_name])
                tables_involved.add(table.name)

            """
            If this is a literal, detect the type after JSON de-serialization.
            Note: JSON really only supports number, str so this type mismatch 
            detection is fairly naive.
            """
            if 'literal' in expression:
                if isinstance(expression['literal'], int):
                    condition_types.add("int")
                else:
                    condition_types.add("str")


        """Check to see that there is no mismatch in the types on LHS, RHS."""
        if len(condition_types) > 1:
            raise Exception('LHS and RHS types do not match {}'.format(list(condition_types)))

        """Check to see how many tables are involved in this condition."""
        if len(tables_involved) > 1:
            late_clauses.append(condition)
        else:
            early_clauses[table.name].append(condition)

    return early_clauses, late_clauses

def evaluate_select(row, query):
    """Grab the fields from the select query and properly order the results."""
    filtered_row = []

    for field in query['select']:
        field_name = '.'.join([field['column']['table'], field['column']['name']])
        filtered_row.append(row[field_name])

    return filtered_row

def execute(query, tables):
    """
    Execute a query given the query JSON and the pre-loaded tables.

        * Confirm that the select clause is semantically correct 
          and has no ambigious column references. 
        * Confirm the where clause is also semantically correct. 
          Split the clausees in to ones that can be evaluated 
          before the join and after the join. 
        * Evaluate the clauses on individual tables if it can be 
          done. 
        * Take the product of the tables that is left, and then 
          evaluate the leftover where clauses.
    """
    try:
        result_headers = validate_and_coalesce_select(query, tables)
    except Exception as e:
        raise Exception("Error in SELECT clause: {}".format(e.args[0]))


    try:
        early_clauses, late_clauses = parse_where_clause(query, tables)
    except Exception as e:
        raise Exception("Error in WHERE clause: {}".format(e.args[0]))

    """
    If there are where clauses that can be evaluated before the join, 
    go ahead and do that now.
    """
    tables_to_join = []
    for table in tables:
        if table.name in early_clauses:
            early_conditions = early_clauses[table.name]
            tables_to_join.append(table.filter(early_conditions))
        else:
            tables_to_join.append(table)


    """
    Take the product of the tables that are left, and evaluate the
    leftover clauses now.
    """
    yield result_headers

    meets_criteria = TableFilter(late_clauses)
    for row in itertools.product(*tables_to_join):
        joined_row = {}
        for t in row:
            joined_row.update(t)

        if meets_criteria(joined_row):
            yield evaluate_select(joined_row, query)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', type=str)
    parser.add_argument('sql_file', type=str)
    parser.add_argument('out_file', type=str)
    args = parser.parse_args()

    query = json.load(open(args.sql_file))

    tables = []
    for table in query['from']:
        tables.append(Table(table['source'],
                            table['as']))


    cursor = execute(query, tables)
    with open(args.out_file, 'w+') as f:
        f.write(json.dumps(list(cursor)))
