import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import core.config.global_var as fd
import traceback

class UserApply(object):

    def __init__(self, conn):
        self.conn = conn

    def user_apply_process(self):
        cur_time = int(time.time())

        with self.conn as db:
            sql = "SELECT * FROM  t_user where member_id > 0";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    type = 0;
                    db.cursor.execute("SELECT id FROM t_member_trade WHERE member_id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) > 0:
                        type = 1
                    if  type == 0:
                        db.cursor.execute("SELECT id FROM sell_member WHERE member_id = %s", (row["member_id"]))
                        members = db.cursor.fetchall();
                        if len(members) > 0:
                            type = 2
                    if  type == 0:
                        continue

                    sql = "INSERT INTO t_user_apply(user_id, apply_member_id, `type` , create_time, version, last_access)  VALUES(%s, %s, %s, %s, %s, %s)"
                    param = (row["id"], "0", str(type) , str(cur_time * 1000), "1" , str(cur_time * 1000))
                    print(param)
                    db.cursor.execute(sql, param)
                    db.conn.commit()
            except:
                traceback.print_exc()
    pass

def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");


if __name__ == '__main__':
    userP = UserApply(fd.conn)
    userP.user_apply_process()