# sqlgenie

Write SQL in your python program as if you were really writing SQL. I've always disliked having SQL queries as string literals and sqlalchemy's ORM-based query language is just too much to remember. Plus most of the time SQL is fine.

```python
import pandas as pd
from sqlalchemy import create_engine
from sqlgenie import *

engine = create_engine('sqlite://')
df = pd.DataFrame({ 'x': [1, 2, 1], 'y': [3, 4, 10], 'z': ['o', 'x', 'p'] })
df.to_sql('test_table', engine, index=False)

test_table = Table(engine, 'test_table')
with db_table(test_table):
    stmt = interpret(lambda *args:
        SQL().SELECT(x, y, z).FROM(test_table).WHERE(lambda *args: x == 9 and y < 8 or z %LIKE% 'o'))
    sql = build_sql_statement(stmt)
    with session_scope(engine) as session:
        res = session.execute(sql)
        print res.fetchall()
```

Hobbyware. Expect bugs. Don't use in prod.
MIT. 2016.
