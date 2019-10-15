from __future__ import print_function
import frappe
from pymysql import InternalError
from six import string_types

def create_index(table, columns, unique=False):
    column_dict = {}
    if isinstance(columns, string_types):
        columns = [columns]
    for i, column_name in enumerate(columns[:]):
        length = None
        try:
            column_name, length = column_name.split('(')
            column_dict[column_name] = length[:-1]
            columns[i] = column_name
        except ValueError:
            pass
    already_indexed = frappe.db.sql('''
        SELECT
            group_concat(column_name order by seq_in_index) seq_idx
        from
            information_schema.statistics
        where
            table_name = %(table_name)s
            and non_unique = %(non_unique)s
        group by
            index_name
        having seq_idx = %(seq_idx)s
    ''', {
        'non_unique': int(not unique),
        'table_name': table,
        'seq_idx': ','.join(columns)
    })
    if already_indexed:
        return
    column_names = f'''"{'","'.join(columns)}"'''
    try:
        try:
            column_def = ', '.join([
                f"`{column_name}`{('(' + column_dict[column_name] + ')') if column_name in column_dict else ''}"
                for column_name in columns
            ])
            frappe.db.sql(f'''
                CREATE
                    {'unique' if unique else ''}
                    index
                    {'unique_' if unique else ''}index_on_{'_'.join(columns)}
                on
                    `{table}`({column_def})
            ''')
            print(f'''Indexed {column_names} columns for table {table}, UNIQUE={unique}''')
            return True
        except InternalError as e:
            print(e)
            if int(e.args[0]) == 1061: #Handle duplicate index error
                pass
            elif int(e.args[0]) == 1170:
                print(f'''Error while creating index on table {table} column {column_names}''')
                print('=>', e)
                print('You should not create keys on columns which are text/blob.')
            else:
                raise
    except Exception as e:
        print(f'Error while creating index on table {table} column {column_names}')
        print('=>', e)