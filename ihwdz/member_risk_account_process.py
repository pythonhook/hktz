#-*- coding: UTF-8 -*-

from decimal import *
import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import core.config.global_var as fd
from xpinyin import Pinyin
import json
import  traceback
from utils.logging import Log

log = Log().getInstance("MemberRiskAccountProcess")
#会员信息
class MemberRiskAccountProcess(object):

    def __init__(self, conn):
        self.conn = conn
        self.py = Pinyin()
        self.cur_time = int(time.time())
        self.__old_table_name="finance_member_amount";
        self.__new_table_name="t_member_risk_account";

    def member_account_process(self):
        with self.conn as db:
            sql = "SELECT * FROM " + self.__old_table_name ;
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    if row["total_amount"] == None:
                        continue

                    # 新的会员是否存在，不存在跳过
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        d = dict()
                        d["account_id"] = row["id"]
                        d["member_id"] = row["member_id"]
                        d["msg"] = "finance_member_amount--对应会员不存在"
                        log.info(d)
                        continue

                    creator = None
                    if row["created_by"] != None:
                        db.cursor.execute("SELECT `name` FROM employee where id = %s", (row["created_by"]))
                        creator = db.cursor.fetchone();

                    sql = "INSERT INTO " + self.__new_table_name + "  VALUES(%s, %s, %s, %s, %s, %s\
                    ,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s)"

                    datetime = None
                    if row["created_time"] != None:
                        datetime = safe_string(row["created_time"])
                    else:
                        datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.cur_time))
                    # 账户关联新的会员ID
                    param = (row["id"], row["member_id"], row["total_amount"], row["available_amount"]\
                            ,10, row["account_period"],"","", "", 1 \
                            ,"" if creator == None else creator["name"], 0 if row["created_by"] == None else row["created_by"] , \
                             int(round(time.mktime(time.strptime(datetime, '%Y-%m-%d %H:%M:%S')) * 1000)), \
                             datetime ,"1", self.cur_time )

                    db.cursor.execute(sql, param)
                    db.conn.commit()
                    #打印结果
                    print(param)
            except:
                traceback.print_exc()
    pass
    # 变更数据
    def member_account_change_log_process(self):

        with self.conn as db:
            sql = "SELECT * FROM  finance_amount_change";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 新的会员是否存在，不存在跳过
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        d = dict()
                        d["finance_amount_change_id"] = row["id"]
                        d["member_id"] = row["member_id"]
                        d["msg"] = "finance_amount_change--对应会员不存在"
                        log.info(d)
                        continue

                    # 账户是否存在，不存在continue
                    db.cursor.execute("SELECT id, available_amount FROM t_member_risk_account WHERE member_id = %s", (row["member_id"]))
                    accounts = db.cursor.fetchall();
                    if len(accounts) == 0:
                        #打印结果
                        d = dict()
                        d["finance_amount_change_id"] = row["id"]
                        d["member_id"] = row["member_id"]
                        d["msg"] = "finance_amount_change--对应账户不存在"
                        log.info(d)
                        continue

                    business_type = 0

                    if row["operation_type"] == 10:
                        business_type = 100
                    elif row["operation_type"] == 20:
                        business_type = 110
                    elif row["operation_type"] == 30:
                        business_type = 120

                    if business_type == 0:
                        log.info("-----------------------error-------------------------------------")
                        continue

                    # json 数据
                    js = dict()
                    amount = 0.00
                    days = 0
                    if row["operation_type"] == 10:   ## 额度变更
                        js["afterAmount"] = 0 if row["after_change_amount"] == None else row["after_change_amount"]
                        js["preAmount"] = 0 if row["before_change_amount"] == None else row["before_change_amount"]
                        #amount = js["afterAmount"] - js["preAmount"]
                        if  row["created_time"] != None:
                            t = int(time.mktime(time.strptime(safe_string(row["created_time"]),'%Y-%m-%d %H:%M:%S')))
                            # 比当前时间大的最小的流水
                            db.cursor.execute("select * from finance_member_trading_flow where unix_timestamp(created_time) > %s  and member_id=%s order by created_time asc limit 1", \
                                                       (t, row["member_id"]));
                            order_up = db.cursor.fetchall();
                            # 比当前时间小的最大的流水
                            db.cursor.execute("select * from finance_member_trading_flow where unix_timestamp(created_time) < %s  and member_id=%s order by created_time desc limit 1", \
                                                       (t, row["member_id"]));
                            order_down = db.cursor.fetchall();
                            js["afterAvilable"] =  js["beforeAvilable"] = 0
                            if len(order_up) > 0 and len(order_down) > 0:
                                js["afterAvilable"] = order_up[0]["before_use_amount"]
                                js["beforeAvilable"] = order_down[0]["after_use_amount"]
                            # elif len(order_up) > 0 and len(order_down) == 0:
                            #     js["afterAvilable"] = order_up[0]["before_use_amount"]
                            #     js["beforeAvilable"] = js["preAmount"]
                            elif len(order_up) == 0 and len(order_down) > 0:
                                js["beforeAvilable"] = order_down[0]["after_use_amount"]
                                js["afterAvilable"] = accounts[0]["available_amount"]
                            else:
                                js["beforeAvilable"] = js["preAmount"]
                                js["afterAvilable"] = js["afterAmount"]

                            amount = js["afterAvilable"] - js["beforeAvilable"]
                    if row["operation_type"] == 20:  ## 账期变更
                        js["afterDays"] = 0 if row["after_change_account_period"] == None else row["after_change_account_period"]
                        js["preDays"] = 0 if row["before_change_account_period"] == None else row["before_change_account_period"]
                        days = js["afterDays"] - js["preDays"]
                    if row["operation_type"] == 30:  ## 额度账期变更
                        js["afterAmount"] = 0 if row["after_change_amount"] == None else row["after_change_amount"]
                        js["preAmount"] = 0 if row["before_change_amount"] == None else row["before_change_amount"]
                        js["afterDays"] = 0 if row["after_change_account_period"] == None else row["after_change_account_period"]
                        js["preDays"] = 0 if row["before_change_account_period"] == None else row["before_change_account_period"]

                        #amount = js["afterAmount"] - js["preAmount"]
                        days = js["afterDays"] - js["preDays"]
                        if  row["created_time"] != None:
                            t = int(time.mktime(time.strptime(safe_string(row["created_time"]),'%Y-%m-%d %H:%M:%S')))
                            # 比当前时间大的最小的流水
                            db.cursor.execute("select * from finance_member_trading_flow where unix_timestamp(`created_time`) > %s  and member_id=%s order by created_time asc limit 1", \
                                                       (t, row["member_id"]));
                            order_up = db.cursor.fetchall();
                            # 比当前时间小的最大的流水
                            db.cursor.execute("select * from finance_member_trading_flow where unix_timestamp(`created_time`) < %s  and member_id=%s order by created_time desc limit 1", \
                                                       (t, row["member_id"]));
                            order_down = db.cursor.fetchall();

                            if len(order_up) > 0 and len(order_down) > 0:
                                js["afterAvilable"] = order_up[0]["before_use_amount"]
                                js["beforeAvilable"] = order_down[0]["after_use_amount"]
                            # elif len(order_up) > 0 and len(order_down) == 0:
                            #     js["afterAvilable"] = order_up[0]["before_use_amount"]
                            #     js["beforeAvilable"] = js["preAmount"]
                            elif len(order_up) == 0 and len(order_down) > 0:
                                js["beforeAvilable"] = order_down[0]["after_use_amount"]
                                js["afterAvilable"] = accounts[0]["available_amount"]
                            else :
                                js["beforeAvilable"] = js["preAmount"]
                                js["afterAvilable"] = js["afterAmount"]

                            amount = js["afterAvilable"] - js["beforeAvilable"]

                    js["msg"] = safe_string(row["operation_content"])

                    creator = None
                    if row["created_by"] != None:
                        db.cursor.execute("SELECT `name` FROM employee where id = %s", (row["created_by"]))
                        creator = db.cursor.fetchone();

                    datetime = None
                    if row["created_time"] != None:
                        datetime = safe_string(row["created_time"])
                    else:
                        datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.cur_time))

                    sql = "INSERT INTO t_member_risk_account_log(account_id, business_type, business_source, order_sn, amount, \
                          days, refere_content_json, creator, creator_id, create_time, create_time_str)  VALUES(%s, %s, %s, %s, %s, %s\
                          ,%s, %s, %s, %s, %s)"

                    param = (accounts[0]["id"], business_type, 0, ""\
                            ,amount ,days, json.dumps(js, ensure_ascii=False), "" if creator == None else creator["name"] , 0 if row["created_by"] == None else row["created_by"] \
                            ,int(round(time.mktime(time.strptime(datetime, '%Y-%m-%d %H:%M:%S')) * 1000)), datetime )

                    db.cursor.execute(sql, param)
                    db.conn.commit()
                    #打印结果
                    print(param)

                db.cursor.execute("DELETE FROM `t_member_risk_account_log` where days = 0 and amount=0")
                db.conn.commit()
            except:
                traceback.print_exc()
    pass

    # 变更数据
    def member_order_flow_process(self):

        with self.conn as db:
            sql = "SELECT * FROM  finance_member_trading_flow";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 新的会员是否存在，不存在跳过
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        d = dict()
                        d["finance_member_trading_flow_id"] = row["id"]
                        d["member_id"] = row["member_id"]
                        d["msg"] = "finance_member_trading_flow对应会员不存在"
                        log.info(d)
                        continue

                    # 账户是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member_risk_account WHERE member_id = %s", (row["member_id"]))
                    account_dict = db.cursor.fetchone();
                    if account_dict == None:
                        #打印结果
                        d = dict()
                        d["finance_member_trading_flow_id"] = row["id"]
                        d["member_id"] = row["member_id"]
                        d["msg"] = "finance_member_trading_flow对应账户不存在"
                        log.info(d)
                        continue

                    if row["business_type"] == 60:
                        continue
                    # json 数据
                    js = dict()
                    js["afterAmount"] = 0 if row["after_use_amount"] == None else row["after_use_amount"]
                    js["preAmount"] = 0 if row["before_use_amount"] == None else row["before_use_amount"]

                    amount = js["afterAmount"] - js["preAmount"]

                    creator = None
                    if row["created_by"] != None:
                        db.cursor.execute("SELECT `name` FROM employee where id = %s", (row["created_by"]))
                        creator = db.cursor.fetchone()

                    datetime = ""
                    create_time = 0
                    if row["created_time"] != None:
                        datetime = safe_string(row["created_time"])
                        create_time = int(round(time.mktime(time.strptime(datetime, '%Y-%m-%d %H:%M:%S')) * 1000));

                    sql = "INSERT INTO t_member_risk_account_log(account_id, business_type, business_source, order_sn, amount, \
                          days, refere_content_json, creator, creator_id, create_time, create_time_str)  VALUES(%s, %s, %s, %s, %s, %s\
                          ,%s, %s, %s, %s, %s)"

                    param = (account_dict["id"], row["business_type"], 0, safe_string(row["order_no"])\
                            ,amount ,0, json.dumps(js, ensure_ascii=False), "" if creator == None else creator["name"] , 0 if row["created_by"] == None else row["created_by"] \
                            ,create_time, datetime)

                    db.cursor.execute(sql, param)
                    db.conn.commit()
                    #打印结果
                    print(param)



            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
    pass

def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");

if __name__ == '__main__':
    member = MemberRiskAccountProcess(fd.conn)
    # 账户
    #member.member_account_process()
    # 变更记录
    #member.member_account_change_log_process()
    # 交易流水
    member.member_order_flow_process()
