#-*- coding: UTF-8 -*-

import traceback
import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import core.config.global_var as fd
from xpinyin import Pinyin

from utils.logging import Log

#会员信息
class UserOrderProcess(object):

    def __init__(self, conn):
        self.conn = conn

    # 订阅单处理
    def user_order_process(self):
        with self.conn as db:
            sql = "SELECT * from user_order";
            try:
                # 获取所有交易会员列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    sql = "select member_id from t_user where id = %s"
                    db.cursor.execute(sql, (row["user_id"]))
                    memberIdMap = db.cursor.fetchone();
                    memberId = 0
                    if memberIdMap == None:
                        continue

                    memberId = memberIdMap["member_id"]

                    memberName = ""
                    if memberId > 0:
                        sql = "select company_full_name from t_member where id = %s"
                        db.cursor.execute(sql, memberId)
                        memberNameMap = db.cursor.fetchone();
                        if memberNameMap != None:
                           memberName = safe_string(memberNameMap["company_full_name"])

                    sql = "update user_order set member_id=%s, member_name=%s  where order_id=%s"
                    param = (memberId, memberName, row["order_id"])
                    db.cursor.execute(sql, param)
                    db.conn.commit()
                    print(param)
            except Exception as e:
                    traceback.print_exc()
    pass



def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");


if __name__ == '__main__':
    uo = UserOrderProcess(fd.conn)
    uo.user_order_process()