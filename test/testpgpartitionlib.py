# Copyright (c) 2010 Matt Harrison

import unittest

import sqlalchemy as sa

import pgpartitionlib

dburl = "postgres://postgres@localhost"
start_part = 1
end_part = 3
new_end = 6

class TestPgpartitionlib(unittest.TestCase):
    def test_main(self):
        ddl = """CREATE TABLE test_part (
        key INTEGER NOT NULL,
        junk NUMERIC(9,0) NOT NULL);"""

        run_sql(dburl, ddl)

        partitioner = pgpartitionlib.Partitioner()

        create_part = partitioner.create_ddl('test_part', 'key', start_part, end_part)
        run_sql(dburl, create_part, autocommit=False)

        func = partitioner.function_code('test_part', 'key', start_part, end_part)
        run_sql(dburl, func, autocommit=False)

        trig = partitioner.trigger_code('test_part', 'key', start_part, end_part)
        run_sql(dburl, trig, autocommit=False)

        #test insert
        insert = """INSERT INTO test_part VALUES(1, 5);"""
        run_sql(dburl, insert)

        select = """SELECT key from test_part_1;"""
        result = run_sql(dburl, select)
        self.assertEqual(list(result), [(1,)])

        
        #test insert bad
        insert = """INSERT INTO test_part VALUES(10, 5);"""
        self.assertRaises(Exception, run_sql, dburl, insert)
        #run_sql(dburl, insert)

        select = """SELECT key from test_part;"""
        result = run_sql(dburl, select)
        self.assertEqual(list(result), [(1,)])
        
        # modify to add up to 6
        # create new tables
        for i in range(end_part, new_end):
            create_part = partitioner.create_ddl('test_part', 'key', i, i+1)
            run_sql(dburl, create_part, autocommit=False)
        # new function - for end values
        func = partitioner.function_code('test_part', 'key', start_part, new_end)
        run_sql(dburl, func, autocommit=False)
        
        #test insert
        insert = """INSERT INTO test_part VALUES(4, 4);"""
        run_sql(dburl, insert)
        select = """SELECT * from test_part;"""
        result = run_sql(dburl, select)
        from decimal import Decimal
        self.assertEqual(list(result), [(1, Decimal('5')), (4, Decimal('4'))])
        

        # test delete
        insert = """DELETE FROM test_part WHERE key=1;"""
        run_sql(dburl, insert)

        select = """SELECT key from test_part;"""
        result = run_sql(dburl, select)
        self.assertEqual(list(result), [(4,)])

        # # drop part
        # drop_part = partitioner.drop_ddl('test_part', 'key', start_part, new_end)
        # run_sql(dburl, drop_part, autocommit=False)

    def tearDown(self):
        # drop part
        partitioner = pgpartitionlib.Partitioner()
        for i in range(start_part, new_end):
            drop_part = partitioner.drop_ddl('test_part', 'key', i, i+1)
            try:
                run_sql(dburl, drop_part, autocommit=False)
            except sa.exceptions.ProgrammingError, e:
                if 'does not exist' in str(e):
                    continue
                else:
                    raise

        drop_master = """DROP table test_part;"""
        run_sql(dburl, drop_master, autocommit=False)
        
def run_sql(dburl, sql, autocommit=True, **kw):
    """
    Allow up to 2 gigs mem usage.
    Postgres doesn't like to vacuum in autocommit, so make it False for that
    """
    engine = sa.create_engine(dburl)
    connection = engine.connect()
    if not autocommit:
        import psycopg2.extensions
        raw = engine.raw_connection()
        connection.connection.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    for key in kw:
        if key in {'work_mem':1, 'maintenance_work_mem':1}:
            connection.execute('set %s = %d;' % (key, kw[key]))
    # for speedy index creation
    #results = connection.execute('set maintenance_work_mem = %d;' % mem)

    results = connection.execute(sql)
    if autocommit:
        connection._commit_impl()
    #connection.commit()
    return results

if __name__ == '__main__':
    unittest.main()
