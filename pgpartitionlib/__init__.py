#!/usr/bin/env python
# Copyright (c) 2010 Matt Harrison
'''
This code will create sql statements to partition a table based an
integer column.  Note that it does not run anything, it only generates
sql.

See http://www.postgresql.org/docs/current/interactive/ddl-partitioning.html
for details

Remember to
run:
CREATE LANGUAGE plpgsql;

update postgres.conf (constraint_exclusion)

Date-based partitioning
=============================

Create DDL
----------

>>> p = MonthPartitioner('test_month', 'date', '2012-01', '2012-04')
>>> print p.create_ddl()
CREATE TABLE test_month_2012-01 (
    CHECK ( date >= '2012-01-01' AND date < '2012-02-01' )
) INHERITS (test_month);
CREATE TABLE test_month_2012-02 (
    CHECK ( date >= '2012-02-01' AND date < '2012-03-01' )
) INHERITS (test_month);
CREATE TABLE test_month_2012-03 (
    CHECK ( date >= '2012-03-01' AND date < '2012-04-01' )
) INHERITS (test_month);

Drop DDL
---------

>>> print p.drop_ddl()
DROP TABLE test_month_2012-01;
DROP TABLE test_month_2012-02;
DROP TABLE test_month_2012-03;

Function Code
---------------

>>> print p.function_code()
CREATE OR REPLACE FUNCTION test_month_insert_function()
RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.date >= '2012-01-01' AND NEW.date < '2012-02-01' ) THEN
        INSERT INTO test_month_2012-01 VALUES (NEW.*);
    ELSIF ( NEW.date >= '2012-02-01' AND NEW.date < '2012-03-01' ) THEN
        INSERT INTO test_month_2012-02 VALUES (NEW.*);
    ELSIF ( NEW.date >= '2012-03-01' AND NEW.date < '2012-04-01' ) THEN
        INSERT INTO test_month_2012-03 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'date out of range.  Fix the test_month_insert_function() function!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

Trigger Code
-----------------

>>> print p.trigger_code()
CREATE TRIGGER insert_test_month_trigger
    BEFORE INSERT ON test_month
    FOR EACH ROW EXECUTE PROCEDURE test_month_insert_function();

Test Int-based partitioning
>>> p = IntPartitioner('test_part', 'adweekid', 0, 2)

Create DDL
--------------

>>> print p.create_ddl() #stmts[0]
CREATE TABLE test_part_0 (
    CHECK ( adweekid >= 0 AND adweekid < 1 )
) INHERITS (test_part);
CREATE TABLE test_part_1 (
    CHECK ( adweekid >= 1 AND adweekid < 2 )
) INHERITS (test_part);

DROP DDL
-----------

>>> print p.drop_ddl()
DROP TABLE test_part_0;
DROP TABLE test_part_1;


INSERT FUNCTION
----------------

>>> print p.function_code()
CREATE OR REPLACE FUNCTION test_part_insert_function()
RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.adweekid >= 0 AND NEW.adweekid < 1 ) THEN
        INSERT INTO test_part_0 VALUES (NEW.*);
    ELSIF ( NEW.adweekid >= 1 AND NEW.adweekid < 2 ) THEN
        INSERT INTO test_part_1 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'adweekid out of range.  Fix the test_part_insert_function() function!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

INSERT TRIGGER
---------------
>>> print p.trigger_code()
CREATE TRIGGER insert_test_part_trigger
    BEFORE INSERT ON test_part
    FOR EACH ROW EXECUTE PROCEDURE test_part_insert_function();


DROP TRIGGER (Cannot CREATE OR REPLACE IT)
--------------------------------------------

>>> print p.drop_trigger_code()
DROP TRIGGER insert_test_part_trigger ON test_part;

INDEX CREATION
----------------

>>> print p.create_idx_ddl()
CREATE INDEX test_part_0_0_index ON test_part_0 (adweekid);
CREATE INDEX test_part_1_0_index ON test_part_1 (adweekid);

INDEX DROPPING
---------------
>>> print p.drop_idx_ddl()
DROP INDEX test_part_0_0_index;
DROP INDEX test_part_1_0_index;


ARBITRARY SQL
--------------

>>> print p.sql('VACUUM ANALYZE {table_name};')
VACUUM ANALYZE test_part_0;
VACUUM ANALYZE test_part_1;

'''
from collections import namedtuple
import datetime as dt
import optparse
import sys
import time

import meta


def month_range(start, end, stride=1):
    """
    >>> list(month_range(dt.date(2012, 11, 1), dt.date(2013, 2, 1)))
    [datetime.date(2012, 11, 1), datetime.date(2012, 12, 1), datetime.date(2013, 1, 1)]
    """
    next_month = start.month
    next_year = start.year
    end_month = end.month
    end_year = end.year
    item = dt.date(next_year, next_month, 1)
    while item < end:
        yield item
        item = add_month(item, stride)


def add_month(date, months=1):
    month = date.month + months
    if month > 12:
        year = date.year + (months/12)+1
        month = month % 12
    else:
        year = date.year
    return dt.date(year, month, 1)


def month_chunk_str(start, end, stride=1, fmt="%Y-%m", out_fmt="%Y-%m-%d"):
    """
    >>> list(month_chunk_str('2012-11', '2013-02'))
    [('2012-11-01', '2012-12-01'), ('2012-12-01', '2013-01-01'), ('2013-01-01', '2013-02-01')]
    """
    start_date = dt.date(*time.strptime(start, fmt)[:3])
    end_date = dt.date(*time.strptime(end, fmt)[:3])
    for chunk in month_chunk(start_date, end_date, stride):
        yield (chunk[0].strftime(out_fmt),chunk[1].strftime(out_fmt))

def month_chunk(start, end, stride=1):
    """
    >>> list(month_chunk(dt.date(2012, 11, 1), dt.date(2013, 2, 1)))
    [(datetime.date(2012, 11, 1), datetime.date(2012, 12, 1)), (datetime.date(2012, 12, 1), datetime.date(2013, 1, 1)), (datetime.date(2013, 1, 1), datetime.date(2013, 2, 1))]
    """
    prev = None
    end = add_month(end)
    for date in month_range(start, end, stride):
        if prev:
            yield prev, date
        prev = date

# sql_* is for a what appears in the CHECK statement
Chunk = namedtuple('Chunk', ['start', 'end', 'suffix', 'sql_start', 'sql_end'])

class MonthChunker(object):
    def __init__(self, start, end, fmt='%Y-%m'):
        self.start = start
        self.end = end
        self.fmt = fmt

    def __iter__(self):
        for item in month_chunk_str(self.start, self.end, fmt=self.fmt):
            suffix = '_' + item[0][:-3]  # don't show day
            sql_start = "'{0}'".format(item[0])
            sql_end = "'{0}'".format(item[1])
            yield Chunk(item[0], item[1], suffix, sql_start, sql_end)

class IntChunker(object):
    def __init__(self, start, end, stride):
        self.start = start
        self.end = end
        self.stride = stride

    def __iter__(self):
        for prev, num in gen_chunks(self.start, self.end, self.stride):
            suffix = '_{0}'.format(prev)
            yield Chunk(prev, num, suffix, prev, num)

class RangePartitioner(object):
    def __init__(self, chunker, table_name, column, index_columns_list=None):
        self.chunker = chunker
        self.table_name = table_name
        self.column = column
        self.index_columns_list = index_columns_list

    def _sql_gen(self, template, start=None, end=None,
                 first_item=None, middle_items=None, last_item=None,
                 do_index=False):
        """
        The template can have the following replacement vars:

        master_table_name - name of table
        column - name of column partitioning on
        start - value for initial range item (ie item >= start)
        end - value for end item (ie item < end)
        table_name - name of partitioned tables
        pos_item - value that can be set with
          first_item - if you need to insert an IF
          middle_items - if you need to insert ELSIF...
          end_item - if you need ELSE
        index_name - name of index on partitioned table (based on column or
                     index_columns_list)
        index_cols - columns where index is placed
        """
        if start:
            stmt = [start.format(master_table_name=self.table_name)]
        else:
            stmt = []
        chunks = list(self.chunker)
        if template:
            cols = self.index_columns_list or [self.column]
            for i, chunk in enumerate(chunks):
                table_name = '{0}{1}'.format(self.table_name, chunk.suffix)
                if i == 0 and first_item:
                    pos_item = first_item
                elif i == len(chunks)-1 and last_item:
                    pos_item = last_item
                else:
                    pos_item = middle_items
                if do_index:
                    for j, col_list in enumerate(cols):
                        index_name = '{0}_{1}_index'.format(table_name, j)
                        col_str = ','.join(cols)
                        stmt.append(template.format(**dict(
                            column=self.column,
                            start=chunk.sql_start,
                            end=chunk.sql_end,
                            master_table_name=self.table_name,
                            table_name=table_name,
                            pos_item=pos_item,
                            index_name=index_name,
                            index_cols=col_str
                            )))
                else:
                    stmt.append(template.format(**dict(
                        column=self.column,
                        start=chunk.sql_start,
                        end=chunk.sql_end,
                        master_table_name=self.table_name,
                        table_name=table_name,
                        pos_item=pos_item
                        )))
        if end:
            stmt.append(end.format(column=self.column,
                master_table_name=self.table_name))
        return '\n'.join(stmt)

    def create_ddl(self):
        temp = """CREATE TABLE {table_name} (
    CHECK ( {column} >= {start} AND {column} < {end} )
) INHERITS ({master_table_name});"""
        return self._sql_gen(temp)

    def drop_ddl(self):
        return self._sql_gen("""DROP TABLE {table_name};""")

    def function_code(self):
        return self._sql_gen("""    {pos_item} ( NEW.{column} >= {start} AND NEW.{column} < {end} ) THEN
        INSERT INTO {table_name} VALUES (NEW.*);""",
            start="""CREATE OR REPLACE FUNCTION {master_table_name}_insert_function()
RETURNS TRIGGER AS $$
BEGIN""",
            end="""    ELSE
        RAISE EXCEPTION '{column} out of range.  Fix the {master_table_name}_insert_function() function!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;""",
            first_item="IF",
            middle_items="ELSIF")

        return self.create_integer_statements(*args, **kw)[2]

    def trigger_code(self):
        return self._sql_gen(None, start="""CREATE TRIGGER insert_{master_table_name}_trigger
    BEFORE INSERT ON {master_table_name}
    FOR EACH ROW EXECUTE PROCEDURE {master_table_name}_insert_function();""")


    def drop_trigger_code(self):
        return self._sql_gen(None, start="""DROP TRIGGER insert_{master_table_name}_trigger ON {master_table_name};""")


    def create_idx_ddl(self, *args, **kw):
        return self._sql_gen("""CREATE INDEX {index_name} ON {table_name} ({index_cols});""",
                             do_index=True)

    def drop_idx_ddl(self, *args, **kw):
        return self._sql_gen("""DROP INDEX {index_name};""",
                             do_index=True)



    def sql(self, sql, start=None, end=None):
        return self._sql_gen(sql, start=start, end=end)


class MonthPartitioner(RangePartitioner):
    def __init__(self, table_name, column, start, end, fmt="%Y-%m"):
        chunker = MonthChunker(start, end, fmt)
        super(MonthPartitioner, self).__init__(chunker, table_name, column)

class IntPartitioner(RangePartitioner):
    def __init__(self, table_name, column, start, end, stride=1):
        chunker = IntChunker(start, end, stride)
        super(IntPartitioner, self).__init__(chunker, table_name, column)

def gen_chunks(start, end, stride):
    """
    generate (start, start+stride) pairs up to end
    >>> list(gen_chunks(0,1,1))
    [(0, 1)]
    >>> list(gen_chunks(0,0,1))
    []
    >>> list(gen_chunks(0,2,1))
    [(0, 1), (1, 2)]
    >>> list(gen_chunks(1,10,3))
    [(1, 4), (4, 7), (7, 10)]
    """
    for i, num in enumerate(xrange(start, end, stride)):
        yield num, num + stride


def _test():
    import doctest
    doctest.testmod()


def main(prog_args):
    parser = optparse.OptionParser(version=meta.__version__)
    parser.add_option('-m', '--master-table', help='specify master table [REQ]')
    parser.add_option('-c', '--column', help='specify partitioning column (should be integer type) [REQ]')
    parser.add_option('--start', help='specify value for first partitioning column value [REQ]')
    parser.add_option('--end', help='specify value for final partitioning column value [REQ]')
    parser.add_option('--stride', default='1', help='specify stride (ie start:1, stride:2 1<= column < 3, 3<= col <5, etc) defaults to 1')
    parser.add_option('--test', action='store_true', help='run doctest')

    parser.add_option('--create-ddl', action='store_true', help='get ddl for partition table creation')
    parser.add_option('--drop-ddl', action='store_true', help='get ddl for partition table dropping')
    parser.add_option('--create-function', action='store_true', help='get ddl for partition table function (trigger calls it, will replace existing funciton)')
    parser.add_option('--create-trigger', action='store_true', help='get ddl for partition table trigger creation')
    parser.add_option('--drop-trigger', action='store_true', help='get ddl for dropping partition table trigger')
    parser.add_option('--create-index-ddl', action='store_true', help='get ddl for partition table creating indexes')
    parser.add_option('--drop-index-ddl', action='store_true', help='get ddl for partition table dropping indexes')
    parser.add_option('--arbitrary-sql', help='specify sql to run against partitions (ie "VACUUM %(table)s;")')

    opt, args = parser.parse_args(prog_args)

    if opt.test:
        _test()
        return

    if not opt.master_table or not opt.column or not opt.start or not opt.end:
        parser.print_help()
        return

    opt.start = int(opt.start)
    opt.end = int(opt.end)
    opt.stride = int(opt.stride)

    p = IntPartitioner(opt.master_table, opt.column, opt.start, opt.end,
                       opt.stride)

    if opt.create_ddl:
        print p.create_ddl()
    if opt.drop_ddl:
        print p.drop_ddl()
    if opt.create_function:
        print p.function_code()
    if opt.create_trigger:
        print p.trigger_code()
    if opt.drop_trigger:
        print p.drop_trigger_code()
    if opt.create_index_ddl:
        print p.create_idx_ddl()
    if opt.drop_index_ddl:
        print p.drop_idx_ddl()
    if opt.arbitrary_sql:
        print p.sql(opt.arbitrary)

if __name__ == '__main__':
    sys.exit(main(sys.argv))

