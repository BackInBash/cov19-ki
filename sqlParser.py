import threading

#
# Mulithreaded SQL Query Parser
#

class Parser(threading.Thread):
    def __init__(self, sql, row, lock, limit):
        threading.Thread.__init__(self)
        self.sql = sql
        self.row = row
        self.lock = lock
        self.limit = limit
    
    def run(self):
        self.limit.acquire()
        self.lock.acquire()

        self.sql = self.sql+"('"+self.row[0]+"','"+self.row[1]+"',"+str(self.row[2])+","+str(self.row[3])+","+str(self.row[4])+","+str(self.row[5]) + \
            ","+str(self.row[6])+","+str(self.row[7])+","+str(self.row[8])+"," + \
            str(self.row[9])+","+str(self.row[10])+","+str(self.row[11])+"),"
        self.lock.release()
        self.limit.release()