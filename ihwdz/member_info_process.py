#-*- coding: UTF-8 -*-

import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import core.config.global_var as fd
from xpinyin import Pinyin
import traceback

#会员信息
class MemberInfoProcess(object):

    def __init__(self, conn):
        self.conn = conn
        self.py = Pinyin()
        self.cur_time = int(time.time())

    #鸿网金融-关联信息表 old->finance_member_affiliate_info    new->t_member_affiliate_info
    def affiliate_info_process(self):
        with self.conn as db:
            sql = "SELECT * FROM finance_member_affiliate_info";
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    sql = "INSERT INTO t_member_affiliate_info(member_id,company_name,shareholder_name,\
                          remark,creator,creator_id,create_time,create_time_str,version,last_access) \
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

                    param = (members[0]["id"],safe_string(row["company_name"]), safe_string(row["shareholder_name"]), safe_string(row["remark"])\
                            ,safe_string(row["created_by_name"]), row["created_by"] \
                            , self.cur_time * 1000, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(self.cur_time))) \
                            ,"1", self.cur_time * 1000 )
                    print(param)
                    db.cursor.execute(sql, param)
                    db.conn.commit()
            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
    pass

    # 鸿网金融-动产抵押 old->finance_chattel_mortgage    new->t_member_chattel_mortgage
    def chattel_mortgage_process(self):
        with self.conn as db:
            sql = "SELECT * FROM finance_chattel_mortgage";
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    creator = None
                    if row["modified_by"] != None:
                        db.cursor.execute("SELECT `name` FROM employee where id = %s", (row["modified_by"]))
                        creator = db.cursor.fetchone()

                    sql = "INSERT INTO t_member_chattel_mortgage(member_id, mortgage_time, amount,\
                           term, content, creator, creator_id, version, last_access) \
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    param = (members[0]["id"],row["mortgage_time"], row["amount"], row["term"]\
                            ,safe_string(row["content"]), "" if creator == None else creator["name"] , row["modified_by"], "1", row["last_access"] )
                    print(param)
                    db.cursor.execute(sql, param)
                    db.conn.commit()
            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
    pass

    # 鸿网金融-对外担保 old->finance_external_guarantee    new->t_member_external_guarantee
    def external_guarantee_process(self):
        with self.conn as db:
            sql = "SELECT * FROM finance_external_guarantee";
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    creator = None
                    if row["modified_by"] != None:
                        db.cursor.execute("SELECT `name` FROM employee where id = %s", (row["modified_by"]))
                        creator = db.cursor.fetchone()

                    sql = "INSERT INTO t_member_external_guarantee(member_id, guarantee_time, amount,\
                           term, content, creator, creator_id, version, last_access) \
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    param = (members[0]["id"],row["guarantee_time"], row["amount"], row["term"]\
                            ,safe_string(row["content"]), "" if creator == None else creator["name"] , row["modified_by"], "1", row["last_access"] )
                    print(param)
                    db.cursor.execute(sql, param)
                    db.conn.commit()
            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
    pass

    # 鸿网金融-会员股东信息表 old->finance_member_shareholder    new->t_member_shareholder
    def shareholder_process(self):
        with self.conn as db:
            sql = "SELECT * FROM finance_member_shareholder";
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    creator = None
                    if row["created_by"] != None:
                        db.cursor.execute("SELECT `name` FROM employee where id = %s", (row["created_by"]))
                        creator = db.cursor.fetchone()

                    sql = "INSERT INTO t_member_shareholder(member_id, shareholder_name, remark,\
                            creator, creator_id, created_time, create_time_str, version, last_access) \
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    param = (members[0]["id"],safe_string(row["shareholder_name"]), safe_string(row["remark"]), "" if creator == None else creator["name"]\
                            ,row["created_by"], int(time.mktime(time.strptime(safe_string(row["created_time"]),'%Y-%m-%d %H:%M:%S'))) \
                            , safe_string(row["created_time"]), "1",  self.cur_time * 1000)
                    print(param)
                    db.cursor.execute(sql, param)
                    db.conn.commit()
            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
    pass

    # 鸿网金融-会员情况说明表 old->finance_situation_description    new->t_member_situation_description
    def situation_description_process(self):
        with self.conn as db:
            sql = "SELECT * FROM finance_situation_description";
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    # 未认证会员不要
                    db.cursor.execute("SELECT id FROM t_member_risk_account WHERE member_id = %s", (members[0]["id"]))
                    accounts = db.cursor.fetchall();
                    if len(accounts) == 0:
                        continue

                    creator = None
                    if row["created_by"] != None:
                        db.cursor.execute("SELECT `name` FROM employee where id = %s", (row["created_by"]))
                        creator = db.cursor.fetchone()

                    sql = "INSERT INTO t_member_situation_description(member_id, need_max_amount, period_type,\
                            account_period, content, inspect_name, inspect_date_str, creator, creator_id, create_time, create_time_str, \
                            content_inferiority, content_advantage, version, last_access) \
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    mktime = 0
                    if row["created_time"] != None:
                        mktime = int(time.mktime(time.strptime(safe_string(row["created_time"]),'%Y-%m-%d %H:%M:%S'))) * 1000

                    param = (members[0]["id"], row["need_max_amount"], row["period_type"], \
                             row["account_period"], safe_string(row["content"]), safe_string(row["inspect_name"]), safe_string(row["inspect_date"]).replace(" 00:00:00", ""), \
                             "" if creator == None else safe_string(creator["name"]) \
                            , row["created_by"], mktime \
                            , safe_string(row["created_time"]), safe_string(row["content_inferiority"]), safe_string(row["content_advantage"]), \
                             "1",  self.cur_time * 1000)

                    db.cursor.execute(sql, param)
                    db.conn.commit()
                    print(param)
            except Exception as e:
                traceback.print_exc()
    pass

    # 鸿网金融-会员诉讼情况表 old->finance_litigation_situation    new->t_member_susong_situation
    def susong_situation_process(self):
        with self.conn as db:
            sql = "SELECT * FROM finance_litigation_situation";
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    creator = None
                    if row["created_by"] != None:
                        db.cursor.execute("SELECT `name` FROM employee where id = %s", (row["created_by"]))
                        creator = db.cursor.fetchone()

                    sql = "INSERT INTO t_member_susong_situation(member_id, litigation_type, litigation_date,\
                            litigation_source, content, creator, creator_id, created_time, create_time_str, version, last_access) \
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    param = (row["member_id"],safe_string(row["litigation_type"]), safe_string(row["litigation_date"]), \
                             row["litigation_source"], safe_string(row["content"]), "" if creator == None else creator["name"]\
                            ,row["created_by"], int(time.mktime(time.strptime(safe_string(row["created_time"]),'%Y-%m-%d %H:%M:%S')))*1000 \
                            , safe_string(row["created_time"]), "1",  self.cur_time * 1000)
                    print(param)
                    db.cursor.execute(sql, param)
                    db.conn.commit()
            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
    pass

    # 鸿网金融-验证项 old->finance_validation    new->t_member_validation
    def validation_process(self):
        with self.conn as db:
            sql = "SELECT * FROM finance_validation";
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    if row["apply_id"] == None:
                        continue
                    # 申请是否存在，不存在continue
                    db.cursor.execute("SELECT member_id FROM finance_amount_apply WHERE id = %s ", (row["apply_id"]))
                    memIdMap = db.cursor.fetchone();
                    if memIdMap == None:
                        continue

                    memberId = memIdMap["member_id"]
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (memberId))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    creator = None
                    if row["created_by"] != None:
                        db.cursor.execute("SELECT `name` FROM employee where id = %s", (row["created_by"]))
                        creator = db.cursor.fetchone()

                    sql = "INSERT INTO t_member_validation(member_id, basic_matching, basic_matching_des,\
                            scene, scene_des,loan,loan_des,monthly_average_flow,flow_matching, creator, creator_id, create_time, create_time_str, version, last_access) \
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    param = (members[0]["id"], row["basic_matching"], safe_string(row["basic_matching_des"]), \
                             row["scene"], safe_string(row["scene_des"]), row["loan"], safe_string(row["loan_des"]),\
                             row["monthly_average_flow"], row["flow_matching"], "" if creator == None else creator["name"]\
                            ,row["created_by"], row["created_time"] \
                            ,  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row["created_time"] / 1000) ), "1",  row["last_access"])
                    print(param)
                    db.cursor.execute(sql, param)
                    db.conn.commit()
            except:
                traceback.print_exc()
    pass







    # 会员-生产信息-设备信息  old->member_factory_device    new->t_member_factory_device
    def member_factory_device_process(self):
        with self.conn as db:
            sql = "SELECT * FROM member_factory_device where valid = 1";
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    sql = "INSERT INTO t_member_factory_device(member_id, device_name, device_brand,\
                           device_grade, performance_index, device_quantity, purchase_date, creator_id, create_time, version, last_access) \
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    param = (members[0]["id"],safe_string(row["device_name"]), safe_string(row["device_brand"]), safe_string(row["device_grade"])\
                            ,safe_string(row["performance_index"]), safe_string(row["device_quantity"]) , safe_string(row["purchase_date"]), \
                             row["creator_id"], row["create_time"], "1", row["last_access"] )
                    print(param)
                    db.cursor.execute(sql, param)
                    db.conn.commit()
            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
    pass

    # 会员-生产信息-填报日记  old->member_factory_fill_log    new->t_member_factory_fill_log
    # def member_factory_fill_log_process(self):
    #     with self.conn as db:
    #         sql = "SELECT * FROM member_factory_fill_log";
    #         try:
    #             db.cursor.execute(sql)
    #             results = db.cursor.fetchall();
    #             for row in results:
    #                 # 会员是否存在，不存在continue
    #                 db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
    #                 members = db.cursor.fetchall();
    #                 if len(members) == 0:
    #                     continue
    #
    #                 sql = "INSERT INTO t_member_factory_fill_log(member_id, admin_id, broker_id,\
    #                        old_content_json, new_content_json, creator_id, create_time) \
    #                        VALUES(%s, %s, %s, %s, %s, %s, %s)"
    #
    #                 param = (row["member_id"],row["admin_id"], row["broker_id"], safe_string(row["old_content_json"])\
    #                         , safe_string(row["new_content_json"]), row["creator_id"] , row["create_time"] )
    #                 print(param)
    #                 db.cursor.execute(sql, param)
    #                 db.conn.commit()
    #         except:
    #             print(sys.exc_info()[0], sys.exc_info()[1])
    # pass

    # 会员-收货地址 old->member_delivery_address    new->t_member_delivery_address
    def delivery_address_process(self):
        with self.conn as db:
            sql = "SELECT * FROM member_delivery_address where valid = 1";
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    sql = "INSERT INTO t_member_delivery_address(member_id, province_code, province_name,\
                           city_code, city_name, district_code, district_name, town_code, town_name, \
                           address, contact_name, mobile, phone, is_default, creator_id, create_time, version, last_access) \
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    param = (members[0]["id"],safe_string(row["province_code"]), safe_string(row["province_name"]), safe_string(row["city_code"])\
                            ,safe_string(row["city_name"]), safe_string(row["district_code"]) , safe_string(row["district_name"]), \
                             safe_string(row["town_code"]), safe_string(row["town_name"]), safe_string(row["address"]), \
                             safe_string(row["contact_name"]), safe_string(row["mobile"]), safe_string(row["phone"]), \
                             row["default_address"], row["creator_id"], row["create_time"], "1", row["last_access"] )
                    print(param)
                    db.cursor.execute(sql, param)
                    db.conn.commit()
            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
    pass

def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");

if __name__ == '__main__':
    memberInfo = MemberInfoProcess(fd.conn)
    # 鸿网金融-关联信息表
    memberInfo.affiliate_info_process()
    # 鸿网金融-动产抵押
    memberInfo.chattel_mortgage_process()
    # 鸿网金融-对外担保
    memberInfo.external_guarantee_process()
    # 鸿网金融-会员股东信息表
    memberInfo.shareholder_process()
    #风控 情况说明
    memberInfo.situation_description_process()
    #风控  诉讼
    memberInfo.susong_situation_process()
    #风控 验证项
    memberInfo.validation_process()




    #收货地址
    memberInfo.delivery_address_process()
    #会员 - 生产信息 - 设备信息
    memberInfo.member_factory_device_process()
    #会员-生产信息-填报日记
