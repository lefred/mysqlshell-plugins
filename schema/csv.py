import csv
from os import access, R_OK
from os.path import isfile
from dateutil import parser
import re
import decimal
import json
from pathlib import Path
from mysqlsh.plugin_manager import plugin, plugin_function


def is_number(s):
    try:
        complex(s)
    except ValueError:
        return False            
    return True

def is_int(s):
    try:
        int(s)
    except ValueError:
        return False
    return True

def is_float(s):
    try:
        float(s)
    except ValueError:
        return False
    return True

def is_signed(s):
    if s.startswith("-"):
        return True
    return False

def is_datetime(s):
    try:
        parser.parse(s)
    except ValueError:
        return False
    return True

def is_date(s):
    if len(s) >= 8 and len(s) <= 10 and ":" not in s:
        return True
    return False

def is_time(s):
    if len(s) >=8 and len(s) <= 11 and ":" in s:
        return True
    return False

def is_json(s):
    try:
        json.loads(s)
    except ValueError as e:
        return False
    return True


@plugin_function("schema_utils.createFromCsv")
def create_from_csv(filename=None, delimiter=',', column_name=True, first_as_pk=True, pk_auto_inc=False, limit=0):
    """
    Generates SQL CREATE TABLE statement from CSV file.

    Args:
        filename (string): The CSV file path.
        delimiter (string): The field delimiter.
        column_name (bool): Use the first row as column name. Default is True
        first_as_pk (bool): Use the first column as Primary Key. Default is True.
        pk_auto_inc (bool): The PK will be defined as int unsigned auto_increment. 
                            If the first_as_pk is false, a new column will be added but invisible. Default is False
        limit (integer): Defines the limit of lines to read form the file. Default: 0, this means no limit.
    """

    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if filename is None:
        filename = shell.prompt("Enter the path and filename of the CSV file : ")

    if not isfile(filename):
        print("File {} doesn't exist !".format(filename))
        return
    if not access(filename, R_OK):
        print("File {} is not readable !".format(filename))
        return

    col = [] 
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=delimiter)
        line_count = 0
        for row in csv_reader:
            if limit > 0 and line_count > limit:
                break
            if len(row[-1]) == 0:
                row.pop()
            if line_count == 0 and column_name:
                for el in row:
                    if column_name:
                        col.append({'name': el})
            else:
                j = 0                
                for el in row:
                    if line_count == 0 and not column_name:
                        col.append({'name': "col{}".format(j)})

                    type = "varchar"
                    if len(el) == 0:
                       if 'type' in col[j]:
                          type = col[j]['type']

                    if is_number(el):
                        if is_int(el):
                            type = "int"
                        elif is_float(el):
                            type = "decimal"
                            d = decimal.Decimal(el)
                            col[j]['digits']=len(d.as_tuple().digits)
                            col[j]['decimal']=abs(d.as_tuple().exponent)
                       
                        if not is_signed(el):
                            col[j]['signed'] = 'unsigned'                            
                        else:
                            col[j]['signed'] = ''                            

                        if 'max' in col[j]:
                            if col[j]['type'] == 'varchar':
                                if len(el) > int(col[j]['max']):
                                    col[j]['max'] = len(el)
                            
                            elif float(el) > float(col[j]['max']):
                                col[j]['max'] = el
                        else:
                            col[j]['max'] = el
                    elif is_datetime(el):
                        type = 'datetime'
                        if is_date(el):
                            type = 'date'
                        elif is_time(el):
                            type = 'time'
                    elif is_json(el):
                        type = 'json'

                    if 'type' in col[j]:
                        if type != 'varchar' and col[j]['type'] == 'varchar':
                            type = 'varchar'

                        if col[j]['type'] != 'varchar' and type == 'varchar':
                            col[j]['max'] = len(col[j]['max'])

                    col[j]['type']= type   
                    if type == "varchar":
                        if 'max' in col[j]:
                            if len(el) > int(col[j]['max']):
                                col[j]['max'] = len(el)
                        else:
                            col[j]['max'] = len(el)

                     #print("name = {}  type = {}  value = {}".format(col[j]['name'], col[j]['type'], el))
                    j += 1
            line_count +=1
    
    table_name = Path(filename).stem.replace(" ","_")
    print("CREATE TABLE {} (".format(table_name))
    j = 1
    for el in col:
        name = el['name']
        name.replace(" ", "_")
        if first_as_pk and j == 1:
            if pk_auto_inc:
                pk = " primary key"
                el['type'] = "int unsigned auto_increment"
            else:
                pk = " primary key"
        else:
            if not first_as_pk and j == 1 and pk_auto_inc:
                print("   id int unsigned auto_increment invisible primary key,")
            pk = ""
        
        type = el['type']
        if type == 'varchar':
            type = 'varchar({})'.format(el['max'])
            if int(el['max']) > 254:
                type = 'text'
        elif type == 'decimal':
            if int(el['digits']) <= int(el['decimal']):
                el['digits'] = int(el['decimal'])+2
            type = 'decimal({},{})'.format(el['digits'], el['decimal'])
        elif type == 'int':
            if int(el['max']) > 2147483647 and el['signed'] != 'unsigned':
                type_int='bigint'
            elif int(el['max']) > 4294967295 and el['signed'] == 'unsigned':
                type_int='bigint'
            elif int(el['max']) > 8388607 and el['signed'] != 'unsigned': 
                type_int='int'
            elif int(el['max']) > 16777215 and el['signed'] == 'unsigned': 
                type_int='int'
            elif int(el['max']) > 32767 and el['signed'] != 'unsigned': 
                type_int='mediumint'
            elif int(el['max']) > 16777215 and el['signed'] == 'unsigned': 
                type_int='mediumint'
            elif int(el['max']) > 127 and el['signed'] != 'unsigned': 
                type_int='smallint'
            elif int(el['max']) > 255 and el['signed'] == 'unsigned': 
                type_int='smallint'
            else:
                type_int='tinyint'
            
            type = '{} {}'.format(type_int, el['signed'])
        if j == len(col):
            comma = ""
        else:
            comma =","
        print("   `{}` {}{}{}".format(name, type, pk, comma))

        j += 1
    print(");") 
    return

