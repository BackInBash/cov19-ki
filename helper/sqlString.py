i = 0
sql = ""
while i < 91:
    sql = sql + "'+row["+str(i)+"]+',"
    i = i+1

print(sql)