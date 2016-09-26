
"""Building arbitray SQL statements from python.
TODO: Support - select, from, where, and, or, limit.
Typical queries to support:
select * from table;
select x, y from table;
select x, y from table where x > 9;
select x, y from table where x > 9 and y < 8;
select x, y, z from table where (x > 9 and y < 8) or z = 9;
select x, y from table limit 10;
select x, y from table order by x desc;
select x, count(*) from table group by y;
functions: count, avg, sum ...

Some conceptual ideas:

SQL().SELECT() -> returns a Statement
Statements have OR, WHERE clauses -> returns ConditionalStatement
Both
    Statements and
    ConditionalStatements
        have GROUP_BY clauses -> returns GroupedStatement or GroupedConditionalStatement
All subclasses of Statements have LIMIT clauses -> returns LimitedStatement
    has no methods
"""

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from contextlib import contextmanager
from collections import OrderedDict
from infix import custom_infix
import unittest
import pandas as pd
import inspect
import re

@contextmanager
def db_table(table):
    frame = inspect.currentframe()
    try:
        for col in table.columns:
            frame.f_globals[col] = col
        yield table
    finally:
        del frame


@contextmanager
def session_scope(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class Column(object):
    def __init__(self, name):
        self.name = name
    def __eq__(self, other):
        return '%s = %s' % (self.name, other.name)


class Table(object):
    def __init__(self, engine, name):
        self.engine = engine
        self.name = name
        self.columns = self.get_columns(engine)

    def get_columns(self, engine):
        _columns = []
        with session_scope(engine) as session:
            query = "select * from %s limit 1" % self.name
            columns = session.execute(query).keys()
        return columns


def parse_lambda(params):
    replacements = [
        ['lambda *args:', ''],
        ['==', '='],
        ['%LIKE%', 'LIKE']
    ]
    for cond in replacements:
        params = params.replace(cond[0], cond[1])
    return params


def unbracket(params):
    return params.replace('(', ' ').replace(')', ' ')


def get_params(op, code):
    if op not in code:
        return ''
    kw = op + '\((.*?)\)'
    try:
        params = re.search(kw, code).group(0)
        raw_params = unbracket(params)
        if op == 'WHERE':
            raw_params = parse_lambda(raw_params)
    except Exception as e:
        print e
    return raw_params


def interpret(*args):
    calls = OrderedDict()
    calls['select'] = 'SELECT'
    calls['from'] = 'FROM'
    calls['where'] = 'WHERE'
    calls['group_by'] = 'GROUP_BY'
    calls['limit'] = 'LIMIT'
    sql_structure = OrderedDict()
    code = inspect.getsource(args[0])
    for key, value in calls.iteritems():
        params = get_params(value, code)
        sql_structure[key] = params
    return sql_structure


def build_sql_statement(sql_structure):
    stmt = ''
    for key, value in sql_structure.iteritems():
        if value == '':
            continue
        else:
            stmt += value
    stmt += ';'
    return stmt


@custom_infix('__rmod__', '__mod__')
def LIKE(a, b):
    return


class SQL(object):
    @staticmethod
    def SELECT(self, *args):
        return self
    @staticmethod
    def FROM(self, *args):
        return self
    @staticmethod
    def WHERE(self, *args):
        return self
    @staticmethod
    def LIMIT(self, *args):
        return self


class TestSQL(unittest.TestCase):

    engine = create_engine('sqlite://')
    df = pd.DataFrame({ 'x': [1, 2, 1], 'y': [3, 4, 10], 'z': ['o', 'x', 'p'] })
    df.to_sql('test_table', engine, index=False)

    def test_create_table(self):
        test_table = Table(self.engine, 'test_table')
        self.assertEqual(test_table.name, 'test_table')

    def test_table_has_correct_columns(self):
        test_table = Table(self.engine, 'test_table')
        self.assertEqual(['x', 'y', 'z'], test_table.columns)

    def test_order(self):
        pass

    def test_usage(self):
        test_table = Table(self.engine, 'test_table')
        with db_table(test_table):
            stmt = interpret(lambda *args: SQL().SELECT(x, y, z).FROM(test_table).WHERE(lambda *args: x == 9 and y < 8 or z %LIKE% 'o'))
            sql = build_sql_statement(stmt)
            with session_scope(self.engine) as session:
                res = session.execute(sql)
                print res.fetchall()

if __name__ == '__main__':
    unittest.main()
