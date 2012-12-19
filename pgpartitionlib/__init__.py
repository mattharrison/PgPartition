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

Test Date-based partitioning
>>> p = MonthPartitioner()
>>> print p.create_ddl('test_month', 'date', '2012-01', '2012-04')


Test Integer based partitioning
>>> p = Partitioner()
>>> stmts = p.create_integer_statements('test_part', 'adweekid', 0, 2, arbitrary_sql='VACUUM ANALYZE %(table)s;')

Create DDL
>>> print stmts[0]
CREATE TABLE test_part_0 (
    CHECK ( adweekid >= 0 AND adweekid < 1 )
) INHERITS (test_part);
CREATE TABLE test_part_1 (
    CHECK ( adweekid >= 1 AND adweekid < 2 )
) INHERITS (test_part);

DROP DDL
>>> print stmts[1]
DROP TABLE test_part_0;
DROP TABLE test_part_1;


INSERT FUNCTION
>>> print stmts[2]
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
>>> print stmts[3]
CREATE TRIGGER insert_test_part_trigger
    BEFORE INSERT ON test_part
    FOR EACH ROW EXECUTE PROCEDURE test_part_insert_function();


DROP TRIGGER (Cannott CREATE OR REPLACE IT)
>>> print stmts[4]
DROP TRIGGER insert_test_part_trigger ON test_part;

INDEX CREATION
>>> print stmts[5]
CREATE INDEX test_part_0_0_index ON test_part_0 (adweekid);
CREATE INDEX test_part_1_0_index ON test_part_1 (adweekid);

INDEX DROPPING
>>> print stmts[6]
DROP INDEX test_part_0_0_index;
DROP INDEX test_part_1_0_index;


ARBITRARY SQL
>>> print stmts[7]
VACUUM ANALYZE test_part_0;
VACUUM ANALYZE test_part_1;

'''
import datetime as dt
import optparse
import sys
import time

import meta

class Partitioner(object):
    def __init__(self):
        # probably should put some state in here
        pass


    def create_ddl(self, *args, **kw):
        return self.create_integer_statements(*args, **kw)[0]

    def drop_ddl(self, *args, **kw):
        return self.create_integer_statements(*args, **kw)[1]

    def function_code(self, *args, **kw):
        return self.create_integer_statements(*args, **kw)[2]

    def trigger_code(self, *args, **kw):
        return self.create_integer_statements(*args, **kw)[3]

    def drop_trigger_code(self, *args, **kw):
        return self.create_integer_statements(*args, **kw)[4]

    def create_idx_ddl(self, *args, **kw):
        return self.create_integer_statements(*args, **kw)[5]

    def drop_idx_ddl(self, *args, **kw):
        return self.create_integer_statements(*args, **kw)[6]

    def sql(self, *args, **kw):
        return self.create_integer_statements(*args, **kw)[7]

    def create_integer_statements(self, master_table_name, column, start, end,
                                  stride=1, prefix='', partition_table_name=None,
                                  index_columns_list=None, arbitrary_sql=None):
        """
        index_columns_list: list of (col1,col2) tuples for index
                            creation, defaults to partitioning column
        arbitrary_sql: say you want to vacuum tables, pass in 'VACUUM %s;'
        """
        partition_table_name = partition_table_name or master_table_name
        index_columns_list = index_columns_list or [(column,)]

        create_stmts = []
        drop_stmts = []
        function_stmts = ["""CREATE OR REPLACE FUNCTION %(table_name)s_insert_function()
RETURNS TRIGGER AS $$
BEGIN""" % dict(table_name=partition_table_name)]
        trigger_stmts = []
        drop_trigger_stmts = []
        create_idx = []
        drop_idx = []
        arb_stmts = []
        for prev, num in gen_chunks(start, end, stride):
            postfix = '_%d' % prev
            individual_table_name = "%(prefix)s%(table_name)s%(postfix)s" % dict(prefix=prefix,
                                        table_name=partition_table_name,
                                        postfix=postfix)
            create_stmts.append("""CREATE TABLE %(table_name)s (
    CHECK ( %(column)s >= %(start)s AND %(column)s < %(end)s )
) INHERITS (%(master_table)s);""" % dict(table_name=individual_table_name,
                                        column=column,
                                        start=prev,
                                        end=num,
                                        master_table=master_table_name))
            drop_stmts.append("""DROP TABLE %(table_name)s;""" %
                              dict(table_name=individual_table_name))

            if prev == start:
                if_or_else = "IF"
            else:
                if_or_else = "ELSIF"
            function_stmts.append("""    %(if_or_else)s ( NEW.%(column)s >= %(start)s AND NEW.%(column)s < %(end)s ) THEN
        INSERT INTO %(table_name)s VALUES (NEW.*);"""%
                                 dict(table_name=individual_table_name,
                                      column=column,
                                      start=prev,
                                      end=num,
                                      if_or_else=if_or_else))

            for j, col_list in enumerate(index_columns_list):
                index_name = "%(table_name)s_%(count)s_index" %dict(table_name=individual_table_name, count=j)
                create_idx.append("""CREATE INDEX %(index_name)s ON %(table_name)s (%(cols)s);""" %
                                  dict(table_name=individual_table_name,
                                       index_name=index_name,
                                       cols=','.join(col_list)))
                drop_idx.append("""DROP INDEX %(index_name)s;""" %
                                  dict(index_name=index_name))

            if arbitrary_sql:
                arb_stmts.append(arbitrary_sql % {'table':individual_table_name})


        function_stmts.append("""    ELSE
        RAISE EXCEPTION '%(column)s out of range.  Fix the %(table_name)s_insert_function() function!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;"""% dict(table_name=partition_table_name,
                             column=column))

        trigger_stmts.append("""CREATE TRIGGER insert_%(table_name)s_trigger
    BEFORE INSERT ON %(table_name)s
    FOR EACH ROW EXECUTE PROCEDURE %(table_name)s_insert_function();"""  % dict(table_name=partition_table_name))

        drop_trigger_stmts.append("""DROP TRIGGER insert_%(table_name)s_trigger ON %(table_name)s;""" % dict(table_name=partition_table_name))
        create = '\n'.join(create_stmts)
        drop = '\n'.join(drop_stmts)
        function = '\n'.join(function_stmts) #already has ;
        trig = '\n'.join(trigger_stmts)
        drop_trig = '\n'.join(drop_trigger_stmts)
        idx = '\n'.join(create_idx)
        del_idx = '\n'.join(drop_idx)
        arb_sql = '\n'.join(arb_stmts)
        return create, drop, function, trig, drop_trig, idx, del_idx, arb_sql


def month_range(start, end, stride=1, fmt="%Y-%m"):
    """
    >>> list(month_range('2012-11', '2013-02'))
    [datetime.date(2012, 11, 1), datetime.date(2012, 12, 1), datetime.date(2013, 1, 1)]
    """
    start_date = dt.date(*time.strptime(start, fmt)[:3])
    next_month = start_date.month
    next_year = start_date.year
    end_date = dt.date(*time.strptime(end, fmt)[:3])
    end_month = end_date.month
    end_year = end_date.year
    item = dt.date(next_year, next_month, 1)
    while item < end_date:
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


def month_chunk(start, end, stride=1, fmt="%Y-%m"):
    """
    >>> list(month_chunk('2012-11', '2013-02'))
    [(datetime.date(2012, 11, 1), datetime.date(2012, 12, 1)), (datetime.date(2012, 12, 1), datetime.date(2013, 1, 1)), (datetime.date(2013, 1, 1), datetime.date(2013, 2, 1))]
    """
    prev = None
    end = add_month(dt.date(*time.strptime(end, fmt)[:3])).strftime(fmt)
    for date in month_range(start, end, stride, fmt):
        if prev:
            yield prev, date
        prev = date


class MonthPartitioner(Partitioner):
    def create_ddl(self, dbname, column, start, end, fmt="%Y-%m", prefix=''):
        stmt = []
        pg_format = "%Y-%m-%d"
        for date_start, date_end in month_chunk(start, end, fmt=fmt):
            pg_start = date_start.strftime(pg_format)
            pg_end = date_end.strftime(pg_format)
            postfix = '_%s' % pg_start[:-3]  # dont' show day
            individual_table_name = "%(prefix)s%(table_name)s%(postfix)s" % dict(prefix=prefix,
                    table_name=dbname,
                    postfix=postfix)
            stmt.append("""CREATE TABLE %(table_name)s (
    CHECK ( %(column)s >= %(start)s AND %(column)s < %(end)s )
) INHERITS (%(master_table)s);""" % dict(table_name=individual_table_name,
                                        column=column,
                                        start=pg_start,
                                        end=pg_end,
                                        master_table=dbname))
        return '\n'.join(stmt)

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

    p = Partitioner()

    kwargs = dict(master_table_name=opt.master_table, column=opt.column, start=opt.start, end=opt.end,
                  stride=opt.stride, arbitrary_sql=opt.arbitrary_sql)

    if opt.create_ddl:
        print p.create_ddl(**kwargs)
    if opt.drop_ddl:
        print p.drop_ddl(**kwargs)
    if opt.create_function:
        print p.function_code(**kwargs)
    if opt.create_trigger:
        print p.trigger_code(**kwargs)
    if opt.drop_trigger:
        print p.drop_trigger_code(**kwargs)
    if opt.create_index_ddl:
        print p.create_idx_ddl(**kwargs)
    if opt.drop_index_ddl:
        print p.drop_idx_ddl(**kwargs)
    if opt.arbitrary_sql:
        print p.sql(**kwargs)

if __name__ == '__main__':
    sys.exit(main(sys.argv))

