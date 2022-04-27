collectList=[]
collectMDSList=[]
plotList=[]

outdir=""
from collections import *
import time as mod_time


def _run_me(session, stmt, header, file_name):
    f_output = open("{}/{}".format(outdir, file_name), 'a')

    result = session.run_sql(stmt)
    tot_cols = result.get_column_count()
    records = result.fetch_all()
    line = ""
    i = 0
    if not header:
        cols = result.get_columns()
        for col in result.get_columns():
            if i == 0:
                line = "{}".format(col.get_column_label())
            else:
                line = "{}\t{}".format(line, col.get_column_label())
            i += 1
        f_output.write(line)

    for record in records:
        i = 0
        line = ""
        while i < tot_cols:
            if i == 0:
                line = "\n{}".format(record[i])
            else:
                line = "{}\t{}".format(line, record[i])
            i += 1
        f_output.write(line)

    f_output.close()
