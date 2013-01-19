from DBUtils.PooledDB import PooledDB
import MySQLdb
from CustomSetting import CustomSetting

class db(object):
    pool = None
    conn = None

    def __new__(cls, *args, **kwargs):
        return super(db, cls).__new__(cls, *args, **kwargs)

    def __init__(self):
        if db.pool is None:
            db.pool = PooledDB(MySQLdb, 10,host = CustomSetting.HOST, user = CustomSetting.USR, passwd = CustomSetting.PWD, db = CustomSetting.DB,charset='utf8')
        self.connect()

    def connect(self):
        self.conn = db.pool.connection()

    def get_cursor(self, how=0):
        try:  
            self.conn.ping()  
        except:  
            self.connect()  
        return self.conn.cursor()

    def get_row(self, *args, **kwargs):
        how = 1 if ('how' in kwargs and kwargs['how'] == 1) else 0
        cursor = self.get_cursor(how)
        try:
            cursor.execute(*args)
        except Exception,e:
            if e[0] == 2013 or e[0] == 2006:
                return self.get_row(*args,**kwargs)
            else:
                raise e
        row = cursor.fetchone()
        cursor.close()
        return row

    def get_rows(self, *args, **kwargs):
        how = 1 if ('how' in kwargs and kwargs['how'] == 1) else 0
        cursor = self.get_cursor(how)
        try:
            cursor.execute(*args)
        except Exception,e:
            if e[0] == 2013 or e[0] == 2006:
                return self.get_rows(*args,**kwargs)
            else:
                raise e
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def execute(self, *args):
        cursor = self.get_cursor()
        try:
            ret = cursor.execute(*args)
        except Exception,e:
            if e[0] == 2013 or e[0] == 2006:
                return self.execute(*args)
            else:
                raise e
        cursor.close()
        return ret

    def commit(self):
        self.conn.commit()

if __name__ == '__main__':
    d = db()
    sql = "SELECT * FROM users WHERE 1 LIMIT 1"
    row = d.get_row(sql)
    print repr(row)
