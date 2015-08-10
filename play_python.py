import sqlite3
import csv
import re
from collections import OrderedDict
import datetime
print "The header to Steve's Program"
x = 5
y = 2
print(" {}+{} = ".format(x, y), x+y)  

def sql_list(l):
    """Returns a string that can be put into an SQL statement.
    """
    print 'SQL Statement: ' , u', '.join('"' + unicode(i) + '"' for i in l)

    return u', '.join('"' + unicode(i) + '"' for i in l)


realpat = re.compile(r"""^      # The start of a string
                         -?     # 0 or 1 negative signs
                         \d*    # 0 or more digits
                         \.     # the decimal point
                         \d*    # 0 or more digits
                         $      # the end of the string
                    """, re.X)

intpat = re.compile(r"""^      # The start of a string
                         -?     # 0 or 1 negative signs
                         \d*    # 0 or more digits
                         $      # the end of the string
                    """, re.X)

def is_int(s):
    return intpat.match(s)
def is_real(s):
    return realpat.match(s)
def find_list_type(col):
    col_is_real = False
    col_is_int = False
    col_is_string = False
    for i in col:
        if i != u'':
            if is_int(i):
                col_is_int = True
            elif is_real(i):
                col_is_real = True
            else:
                return u'text'
            # Now check if we have agreement with what we've found so far
            if (col_is_int == True) and (col_is_real == True):
                col_is_real = True      # If there is a mix of ints and reals, the column is real
                col_is_int = False

    if col_is_int == True:
        return u'integer'
    elif col_is_real == True:
        return u'real'
    else:
        return u'text'

def import_csv_str_to_db(s, delim, tablename, conn):
    c = conn.cursor()
    l = s.splitlines()
    r = csv.DictReader(l, delimiter=delim)

    print 'c= ' , c , 'l= ' , l , 'r= ' , r
    # Create temporary table.
    # First, create a list of header definitions
    header_defs = []
    for label in r.fieldnames:
        # If we get an ID row from the csv, treat it as a primary key
        if (label == 'id' or label == 'ID'):
            header_defs.append(u'id integer primary key')
        else:
            header_defs.append(label + u' text')
    
    header = u', '.join(header_defs)
    cmd = u'CREATE TEMP TABLE %s (%s)' % (tablename, header)
    c.execute(cmd)
    
    print header
    print cmd
    
    import_DictReader_to_db(r, tablename, c)
    conn.commit()
    print '############## LEAVING import_csv_str_to_db ##############'
    # # Create a new table with appropriate containers
    # header = ', '.join(['%s %s' % (label[0], label[1]) for label in types])
    # cmd = 'CREATE TABLE %s (id integer primary key, %s)' % (tablename, header,)
    # c.execute(cmd)
    # r = csv.DictReader(l, delimiter=';')
    # import_DictReader_to_db(r, tablename, c)
    # conn.commit()
    # c.execute('DROP TABLE %s' % (temp_table_name,))
    # conn.commit()
def import_DictReader_to_db(reader, dbname, c):
    ld = [record for record in reader]
    print 'ld= ' , [record for record in ld]
    for record in ld:
        keys = []
        values = []
        for k, v in record.iteritems():
            print 'keys.append(k)=' , keys.append(k)
            print 'values.append(v)=', values.append(v)

        cmd = u"INSERT INTO %s (%s) values (%s)" % (dbname, sql_list(keys), sql_list(values))
        print cmd
        
        try:
            c.execute(cmd)
        except:
            print 'This command failed:'
            print cmd
            break

def detect_column_types(conn, tablename, pragmaname=u'temp_pragma'):
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Get names of each column
    c.execute(u"pragma table_info(%s)" % tablename)
    print 'pragma table_info= ' , u"pragma table_info(%s)" % tablename
    pragma = c.fetchall()
    print 'pragma= ' , pragma
    # Create a copy of the pragma table
    header = u'cid integer primary key, name text, type text, notn integer, dflt text, pk integer'
    print 'header= ' , header
    c.execute(u'CREATE TABLE %s (%s)' % (pragmaname, header))
    print 'pragmaname= ' , pragmaname
    # The 6th column is 1 if the column is a primary key, 0 if not.
    # Scan the results for a primary key.
    # Now figure out what kind of container each column needs
    for row in pragma:
        rowdata = [data for data in row]
        if row['pk'] == 0:
            cmd = u"Select %s from %s" % (row['name'],tablename)
            print 'loop cmd= ' , cmd
            results = [unicode(item[0]) for item in c.execute(cmd)]
            print 'loop results ' , results
            rowdata[2] = find_list_type(results)
            print 'loop rowdata[2]= ' , rowdata[2]
        c.execute(u'INSERT INTO %s VALUES (%s)' % (pragmaname, sql_list(rowdata)))
    print '############### LEAVING DETECT COLUMN TYPES ##################' 
def change_column_type(conn, tablename, colname, newtype, pragmaname=u'temp_pragma'):
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    print 'change column type= ' , """UPDATE %s SET type="%s" WHERE name="%s" """ % (pragmaname, newtype, colname)
    c.execute(u"""UPDATE %s SET type="%s" WHERE name="%s" """ % (pragmaname, newtype, colname))
    print '########### LEAVING CHANGE COLUMN TYPE ############'
def create_table(conn, tablename, pragmaname='temp_pragma'):
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    # Get info from pragma
    c.execute("select pk, name || ' ' || type as nametype from temp_pragma")
    print "select pk, name || ' ' || type as nametype from temp_pragma"
    pragma = c.fetchall()
    print pragma
    headerlist = []
    for row in pragma:
        if row['pk'] == 1:
            headerlist.append(row['nametype'] + ' primary key')
            print row['nametype'] + ' primary key'
        else:
            headerlist.append(row['nametype'])
            print 'row[nametype]=' , row['nametype']
    header = u', '.join(headerlist)
    print 'header from create table=' , header
    cmd = u'CREATE TABLE %s (%s)' % (tablename, header,)
    print 'cmd from create table= ' , cmd
    c.execute(cmd)
    print '###################### LEAVING CREATE TABLE ###############

if __name__=='__main__':
    print 'Is real' , is_real('Testing')
    print 'Is real' , is_real('1.00')
    print 'Is real' , is_real('1')
    print 'Is int ' , is_int('9')
    print 'Is int ' , is_int('1.095768')
    print 'Is int ' , is_int('pooping')
    print 'Find list type:' , find_list_type('3.4562')
    print 'Find list type:' , find_list_type('Steve')
    print 'Find list type:' , find_list_type('7')
    conn = sqlite3.connect(':memory:')
    c = conn.cursor()

    temp_table_name = 'temp_data'
    tablename = 'data'

    with open('data.csv') as fin:
        s = fin.read()
        print s
        import_csv_str_to_db(s,';',temp_table_name,conn)
    detect_column_types(conn,temp_table_name)
    # Change the type of the 'BILL_RUN_DATE' column to text:
    change_column_type(conn, 'data','BILL_RUN_DATE', u'text')

    create_table(conn, tablename)


