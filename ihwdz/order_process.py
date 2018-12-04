#-*- coding: UTF-8 -*-

import traceback
import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import core.config.global_var as fd
from xpinyin import Pinyin

from utils.logging import Log

log = Log().getInstance("OrderProcess")
#会员信息
class OrderProcess(object):

    def __init__(self, conn):
        self.conn = conn

    # order
    def order_process(self):
        with self.conn as db:
            sql = "SELECT id, member_id, member_name, user_id, user_name, user_phone,sell_member_id,sell_member_name FROM `order` ";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 新会员
                    sql = "SELECT id FROM t_member WHERE company_full_name = %s";
                    db.cursor.execute(sql, (safe_string(row["member_name"])))
                    memberMap = db.cursor.fetchone();
                    if memberMap == None:
                        log.info(safe_string(row["member_name"]) + ":未找到相应的会员")
                        continue

                    # 新的用户
                    sql = "SELECT id, user_account,`name` FROM t_user WHERE member_id = %s";
                    db.cursor.execute(sql, (memberMap["id"]))
                    users = db.cursor.fetchall();
                    if len(users) == 0:
                        log.info(safe_string(row["member_name"]) + ":未找到相应的用户")
                        continue

                    uid = 0
                    uname = ""
                    user_account = ""
                    for u in users:
                        if u["id"] == row["user_id"]:
                            uid = row["user_id"]
                            uname = safe_string(row["user_name"])
                            user_account = safe_string(row["user_phone"])
                            break;
                    if uid == 0:
                        uid = users[0]["id"]
                        uname = safe_string(users[0]["name"])
                        user_account = safe_string(users[0]["user_account"])

                    # 新商家ID
                    sql = "SELECT id FROM t_member WHERE company_full_name = %s";
                    db.cursor.execute(sql, (safe_string(row["sell_member_name"])))
                    sellMemberMap = db.cursor.fetchone();
                    if sellMemberMap == None:
                        log.info(safe_string(row["sell_member_name"]) + ":未找到相应的商家")
                        continue

                    if row["member_id"] != memberMap["id"]:
                        sql = "update `order`  set member_id = %s where id = %s"
                        param = (memberMap["id"], row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        d = dict()
                        d["table"] = "order"
                        d["order_id"] = row["id"]
                        d["old_member_id"] = row["member_id"]
                        d["new_member_id"] = memberMap["id"]
                        d["member_name"] = safe_string(row["member_name"])
                        log.info(d)

                    if row["sell_member_id"] != sellMemberMap["id"]:
                        sql = "update `order`  set sell_member_id = %s where id = %s"
                        param = (sellMemberMap["id"], row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        d = dict()
                        d["table"] = "order"
                        d["order_id"] = row["id"]
                        d["old_sell_member_id"] = row["sell_member_id"]
                        d["new_sell_member_id"] = sellMemberMap["id"]
                        d["sell_member_name"] = safe_string(row["sell_member_name"])
                        log.info(d)

                    if row["user_id"] != uid:
                        sql = "update `order` set user_id = %s,user_name=%s, user_phone=%s where id = %s"
                        param = (uid, uname, user_account, row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        d = dict()
                        d["table"] = "order"
                        d["order_id"] = row["id"]
                        d["old_user_id"] = row["user_id"]
                        d["old_user_name"] = safe_string(row["user_name"])
                        d["old_user_phone"] = safe_string(row["user_phone"])
                        d["new_user_id"] = uid
                        d["new_user_name"] = uname
                        d["new_user_phone"] = user_account
                        log.info(d)
            except Exception as e:
                traceback.print_exc()

    pass

    # invoice
    def invoice_process(self):
        with self.conn as db:
            sql = "SELECT id, order_id,member_id, member_name FROM  invoice";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    sql = "SELECT member_id, member_name FROM `order` WHERE id = %s limit 1";
                    db.cursor.execute(sql, (row["order_id"]))
                    orders = db.cursor.fetchall();
                    if len(orders) == 0 :
                        continue

                    member_id = orders[0]["member_id"]
                    member_name = safe_string(orders[0]["member_name"])

                    if row["member_id"] != member_id:
                        sql = "update invoice  set member_id = %s, member_name=%s where id = %s"
                        param = (member_id, member_name, row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        #打印结果
                        d = dict()
                        d["table"] = "voice"
                        d["invoice_id"] = row["id"]
                        d["old_member_id"] = row["member_id"]
                        d["old_member_name"] = safe_string(row["member_name"])
                        d["new_member_id"] = member_id
                        d["new_member_name"] = member_name
                        log.info(d)
            except Exception as e:
                    traceback.print_exc()
    pass

    # order_history
    def order_history_process(self):
        with self.conn as db:
            sql = "SELECT id, order_id,member_id, member_name, user_id, user_name, user_phone, sell_member_id, sell_member_name  FROM order_history" ;
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    sql = "SELECT member_id, member_name, user_id, user_name,user_phone, sell_member_id, sell_member_name FROM `order` WHERE id = %s limit 1";
                    db.cursor.execute(sql, (row["order_id"]))
                    orders = db.cursor.fetchall();
                    if len(orders) == 0 :
                        continue

                    member_id = orders[0]["member_id"]
                    member_name = safe_string(orders[0]["member_name"])
                    user_id = orders[0]["user_id"]
                    user_name = safe_string(orders[0]["user_name"])
                    user_phone = safe_string(orders[0]["user_phone"])
                    sell_member_id = orders[0]["sell_member_id"]
                    sell_member_name = safe_string(orders[0]["sell_member_name"])

                    if row["member_id"] != member_id:
                        sql = "update order_history  set member_id = %s, member_name=%s where id = %s"
                        param = (member_id, member_name, row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        d = dict()
                        d["table"]="order_history"
                        d["order_id"] = row["id"]
                        d["old_member_id"] = row["member_id"]
                        d["new_member_id"] = member_id
                        d["old_member_name"] = safe_string(row["member_name"])
                        d["new_member_name"] = member_name
                        log.info(d)

                    if row["sell_member_id"] != sell_member_id:
                        sql = "update order_history  set sell_member_id = %s,sell_member_name=%s where id = %s"
                        param = (sell_member_id, sell_member_name, row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        d = dict()
                        d["table"] = "order_history"
                        d["order_history_id"] = row["id"]
                        d["old_sell_member_id"] = row["sell_member_id"]
                        d["new_sell_member_id"] = sell_member_id
                        d["old_sell_member_name"] = safe_string(row["sell_member_name"])
                        d["new_sell_member_name"] = sell_member_name
                        log.info(d)
                    if row["user_id"] != user_id:
                        sql = "update order_history set user_id = %s,user_name=%s,user_phone=%s where id = %s"
                        param = (user_id, user_name, user_phone, row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        d = dict()
                        d["table"] = "order_history"
                        d["order_history_id"] = row["id"]
                        d["old_user_id"] = row["user_id"]
                        d["old_user_name"] = safe_string(row["user_name"])
                        d["old_user_phone"] = safe_string(row["user_phone"])
                        d["new_user_id"] = user_id
                        d["new_user_name"] = user_name
                        d["new_user_phone"] = user_phone
                        log.info(d)
            except Exception as e:
                traceback.print_exc()
    pass

    # order_pay
    def order_pay_process(self):
        with self.conn as db:
            sql = "SELECT id, order_id, sell_member_id,sell_member_name  FROM  order_pay";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    sql = "SELECT sell_member_id, sell_member_name FROM `order` WHERE id = %s limit 1";
                    db.cursor.execute(sql, (row["order_id"]))
                    orders = db.cursor.fetchall();
                    if len(orders) == 0 :
                        continue

                    sell_member_id = orders[0]["sell_member_id"]
                    sell_member_name = safe_string(orders[0]["sell_member_name"])

                    if row["sell_member_id"] != sell_member_id:
                        sql = "update order_pay  set sell_member_id = %s, sell_member_name=%s where id = %s"
                        param = (sell_member_id, sell_member_name, row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        #打印结果
                        d = dict()
                        d["table"] = "order_pay"
                        d["order_pay_id"] = row["id"]
                        d["old_sell_member_id"] = row["sell_member_id"]
                        d["old_sell_member_name"] = safe_string(row["sell_member_name"])
                        d["new_sell_member_id"] = sell_member_id
                        d["new_member_name"] = sell_member_name
                        log.info(d)
            except Exception as e:
                    traceback.print_exc()
    pass

    # overdue_urge  逾期催收表
    def overdue_urge_process(self):
        with self.conn as db:
            sql = "SELECT id, order_id, member_id FROM  overdue_urge";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    sql = "SELECT member_id FROM `order` WHERE id = %s limit 1";
                    db.cursor.execute(sql, (row["order_id"]))
                    orders = db.cursor.fetchall();
                    if len(orders) == 0 :
                        continue
                    member_id = orders[0]["member_id"]
                    if row["member_id"] != member_id:
                        sql = "update overdue_urge  set member_id = %s where id = %s"
                        param = (member_id, row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        #打印结果
                        d = dict()
                        d["table"] = "overdue_urge"
                        d["overdue_urge_id"] = row["id"]
                        d["old_member_id"] = row["member_id"]
                        d["new_member_id"] = member_id
                        log.info(d)
            except Exception as e:
                    traceback.print_exc()
    pass

    # receivable_bill  到账单
    def receivable_bill_process(self):
        with self.conn as db:
            sql = "SELECT id,  member_id, member_name FROM  receivable_bill";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    sql = "SELECT id FROM t_member WHERE company_full_name = %s limit 1";
                    db.cursor.execute(sql, (safe_string(row["member_name"])))
                    members = db.cursor.fetchall();
                    if len(members) == 0 :
                        log.info(safe_string(row["member_name"]) + ":未找到相应的会员")
                        continue

                    member_id = members[0]["id"]

                    if row["member_id"] != member_id:
                        sql = "update receivable_bill  set member_id = %s where id = %s"
                        param = (member_id, row["id"])
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        #打印结果
                        d = dict()
                        d["table"] = "receivable_bill"
                        d["receivable_bill_id"] = row["id"]
                        d["old_member_id"] = row["member_id"]
                        d["new_member_id"] = member_id
                        log.info(d)
            except Exception as e:
                    traceback.print_exc()
    pass

def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");


if __name__ == '__main__':
    order = OrderProcess(fd.conn)
    order.order_process()
    # order.order_history_process()
    # order.invoice_process()
    # order.order_pay_process()
    # order.overdue_urge_process()
    # order.receivable_bill_process()
