
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
"""

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from contextlib import contextmanager
from collections import namedtuple
import unittest
import pandas as pd
import inspect
import sympy


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
        logging.info("rolling back transaction")
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
        for i in columns:
            _columns.append(Column(i))
        return columns


class Cond(object):
    def __init__(self, condition):
        self.condition = condition
    # looks like and cannot be overridden
    def AND(self):
        pass
    def OR(self):
        pass


class Fragment(object):
    def __init__(self):
        self.fragment = fragment


def interpret(*args):
    # need to build a parser here
    return inspect.getsource(args[0])


class Statement(object):

    def __init__(self):
        self.statement = ""

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
    def AND(self, *args):
        return self

    @staticmethod
    def OR(self, *args):
        return self



class TestSQL(unittest.TestCase):

    engine = create_engine('sqlite://')
    df = pd.DataFrame({ 'x': [1, 2], 'y': [3, 4], 'z': [5, 6] })
    df.to_sql('test_table', engine, index=False)

    def test_create_table(self):
        test_table = Table(self.engine, 'test_table')
        self.assertEqual(test_table.name, 'test_table')

    def test_table_has_correct_columns(self):
        test_table = Table(self.engine, 'test_table')
        #self.assertEqual(['x', 'y', 'z'], test_table.columns)
        pass

    def test_order(self):
        pass

    def test_usage(self):
        test_table = Table(self.engine, 'test_table')
        with db_table(test_table) as t:
            statement = interpret(lambda *args: Statement().SELECT(x, y, z))
            result = statement.execute(engine)


    def test_where(self):
        # override builtins, create a Column class
        # s.SELECT(x, y, z).FROM(t).WHERE((x > 7 and y < 9) or z.like('bla'))
        # the 'and' between the conditions are problematic here:
        # s.SELECT(x, y, z).FROM(t).WHERE((t.x > 7 and t.y < 9) or t.z <= 5 and t.z.like('%bla'))
        # s.SELECT(x, y, z).FROM(t).WHERE(lambda x, y: x > 7 and y < 9 and z.like('bla'))
        # or using https://pypi.python.org/pypi/infix/
        # s.SELECT(x, y, z).FROM(t).WHERE(x %gt% 7 %and% y %like% '%gre')
        # or _just_ define %like% that way
        # or sympy: http://docs.sympy.org/latest/tutorial/intro.html
        #
        pass

if __name__ == '__main__':
    unittest.main()
