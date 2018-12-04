#-*- coding: UTF-8 -*-

import traceback
import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import core.config.global_var as fd
from xpinyin import Pinyin

from utils.logging import Log
log = Log().getInstance("PopProcess")
#会员信息
class PopProcess(object):

    def __init__(self, conn):
        self.conn = conn

    # 会员名称
    def pop_process(self):
        log.info("pop.sell_member   update member_id start...........")
        with self.conn as db:
            sql = "SELECT id,member_id, member_name FROM  sell_member";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    sql = "SELECT id FROM t_member WHERE company_full_name = %s";
                    db.cursor.execute(sql, (safe_string(row["member_name"]).replace(' ', '')))
                    memberMap = db.cursor.fetchone();
                    if memberMap == None :
                        continue

                    if row["member_id"] != memberMap["id"]:
                        sql = "update sell_member  set member_id = %s where id = %s"
                        param = (memberMap["id"], row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        #打印结果
                        d = dict()
                        d["sell_member_id"] = row["id"]
                        d["old_member_id"] = row["member_id"]
                        d["new_member_id"] = memberMap["id"]
                        d["member_name"] = safe_string(row["member_name"])
                        log.info(d)
            except Exception as e:
                    traceback.print_exc()
        log.info("pop.sell_member   update member_id end...........")
    pass

    # 会员公共信息
    def shop_process(self):
        log.info("pop.shop   update member_id start...........")
        with self.conn as db:
            sql = "SELECT * FROM shop" ;
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    sql = "SELECT id FROM t_member WHERE company_full_name = %s";
                    db.cursor.execute(sql, (safe_string(row["name"]).replace(' ', '')))
                    memberMap = db.cursor.fetchone();
                    if memberMap == None :
                        continue

                    if row["member_id"] != memberMap["id"]:
                        sql = "update shop  set member_id = %s where id = %s"
                        param = (memberMap["id"], row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()

                        d = dict()
                        d["shop_id"] = row["id"]
                        d["old_member_id"] = row["member_id"]
                        d["new_member_id"] = memberMap["id"]
                        d["member_name"] = safe_string(row["name"])
                        log.info(d)
            except Exception as e:
                traceback.print_exc()
        log.info("pop.shop   update member_id end...........")
    pass



def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");


if __name__ == '__main__':
    pop = PopProcess(fd.conn)
    pop.pop_process()
    pop.shop_process()