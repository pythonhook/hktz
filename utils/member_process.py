#-*- coding: UTF-8 -*-

import datetime
import time
import sys

class MemberProcess(object):

    def __init__(self, conn):
        self.conn = conn

    def process_users(self):
        with self.conn as db:
            sql = "SELECT * FROM t_account_temp";               # 取出老数据
            now = int(time.time()) * 1000
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    account_no = row["account_no"].decode("utf-8")
                    db.cursor.execute("select id from t_account where account_no = '" + account_no + "' ")
                    r = db.cursor.fetchone()

                    if r == None:

                        sql = "insert into t_member(editor_id, create_time, last_access, version) values(%s, %s, %s, %s)"
                        param = (0, now, now, 1)
                        db.cursor.execute(sql, param);
                        db.conn.commit()

                        print(sql)

                        db.cursor.execute("select Max(id) as id from t_member ")
                        max_id = db.cursor.fetchone()
                        member_id = max_id["id"]
                        status =  0 if row["status"] == 10 else  1
                        registered_time = int(time.mktime(time.strptime(str(row["registered_time"]), '%Y-%m-%d %H:%M:%S'))) * 1000 if  row["registered_time"] else 0
                        last_login_time = int(time.mktime(time.strptime(str(row["last_login_time"]), '%Y-%m-%d %H:%M:%S')))*1000 if  row["last_login_time"] else 0
                        created_time = int(time.mktime(time.strptime(str(row["created_time"]), '%Y-%m-%d %H:%M:%S')))*1000 if  row["created_time"] else 0

                        sql = "insert into t_account(member_id, account_no, phone, name, password,sex,status,main_account_flag,register_source,registered_time, \
                            last_service_date, last_login_time, login_num, editor_id, last_access, creator_id, create_time, version) values(%s, \
                            %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)"
                        param = (member_id, row["account_no"], row["phone"], row["name"].decode("utf-8"), row["password"], 0 , 10, 1, row["register_source"], \
                                 registered_time, 0, last_login_time, 0, 0, now, 0, created_time, 1)

                        db.cursor.execute(sql, param);
                        db.conn.commit()

            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
        pass
