#-*- coding: UTF-8 -*-

import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import core.config.global_var as fd

class UserProcess(object):

    def __init__(self, conn):
        self.conn = conn
        self.__old_table_name="account";
        self.__new_table_name="t_user";

    def user_process(self):

        cur_time = int(time.time())
        cur_short_time = time.strftime('%Y-%m-%d', time.localtime(cur_time))
        cur_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cur_time))

        with self.conn as db:
            sql = "SELECT * FROM " + self.__old_table_name ;
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    if row["member_id"] == None:
                        continue

                    member_id = 0
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) > 0:
                        member_id = row["member_id"]

                    sql = "INSERT INTO " + self.__new_table_name + "  VALUES(%s, %s, %s, %s, %s, %s, \
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    if row["status"] == 10:
                        row["status"] = 0

                    param = (row["id"], member_id, safe_string(row["account_no"]), safe_string(row["phone"]), safe_string(row["password"]),safe_string(row["name"]) \
                             , row["sex"], row["status"], "", 0 if member_id == 0 else row["main_account_flag"], 40 if row["register_source"] == None else row["register_source"], safe_string(row["position"]) \
                             , safe_string(row["qq"]), safe_string(row["wechat"]), safe_string(row["email"]), safe_string(row["address"]), \
                             safe_string(row["post_code"]), safe_string(row["province_code"]) \
                             , safe_string(row["province_name"]), safe_string(row["city_code"]), safe_string(row["city_name"]), safe_string(row["district_code"]), \
                             safe_string(row["district_name"]), row["create_time"] \
                             , time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(row["create_time"])/1000)) , "1", row["last_access"])

                    db.cursor.execute(sql, param)
                    db.conn.commit()
                    # 打印结果
                    print(param)
            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
    pass

    # def u_member_id_process(self):
    #     with self.conn as db:
    #         sql = "SELECT * FROM " + self.__new_table_name;
    #         try:
    #             # 获取所有记录列表
    #             db.cursor.execute(sql)
    #             results = db.cursor.fetchall()
    #             for row in results:
    #                 db.cursor.execute("SELECT * FROM  t_member where id = %s", (row["member_id"]))
    #                 members = db.cursor.fetchall();
    #                 if len(members) == 0:
    #                     db.cursor.execute("update  t_user set member_id = 0 where id = %s", (row['id']))
    #                     db.conn.commit()
    #                     # 打印结果
    #                     print(sql)
    #         except:
    #             print(sys.exc_info()[0], sys.exc_info()[1])
    # pass

def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");

def safe_int(object):
    return  0  if object == None  else object;


if __name__ == '__main__':
    user = UserProcess(fd.conn)
    user.user_process()
