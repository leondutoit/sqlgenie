
"""Building arbitray SQL statements from python.
TODO: Support - select, from, where, and, or, limit.
"""

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from contextlib import contextmanager
from collections import namedtuple
import unittest
import pandas as pd
import inspect


@contextmanager
def db_table(table):
    frame = inspect.currentframe()
    try:
        for col in table.columns:
            frame.f_globals[col] = str(col)
        yield table
    finally:
        del frame


@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
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


class Table(object):

    # inspect module: https://docs.python.org/2/library/inspect.html

    def __init__(self, engine, name):
        self.engine = engine
        self.name = name
        self.columns = self.get_columns(engine)
        self.statement = None

    def get_columns(self, engine):
        with session_scope(engine) as session:
            query = "select * from %s limit 1" % self.name
            columns = session.execute(query).keys()
        return columns

    def execute(self):
        pass


class Statement(object):

    # maybe the statement should have an execute method

    def __init__(self, table):
        self.table = table
        self.statement = ""

    def SELECT(self, *args):
        return

    def FROM(self, *args):
        _add(args)
        return


class TestSQL(unittest.TestCase):

    engine = create_engine('sqlite://')
    df = pd.DataFrame({ 'x': [1, 2], 'y': [3, 4], 'z': [5, 6] })
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
        with db_table(test_table) as t:
            s = Statement()
            res = s.SELECT(x, y, z).FROM(t).execute()

    def test_where(self):
        # s.SELECT(x, y, z).FROM(t).WHERE((x.gt(7) and y.lt(9)) or z.let(5)).execute()
        # s.SELECT(x, y, z).FROM(t).WHERE((x > 7 and y < 9) or z <= 5).execute()
        # pass in a lambda?


if __name__ == '__main__':
    unittest.main()
