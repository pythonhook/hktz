#-*- coding: UTF-8 -*-

import traceback
import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import core.config.global_var as fd
from xpinyin import Pinyin

#会员信息
class MemberProcess(object):

    def __init__(self, conn):
        self.conn = conn
        self.__old_table_name="member";
        self.__new_table_name="t_member";

    # 会员名称
    def member_process(self):
        py = Pinyin()
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
                    member_name = safe_string(row["company_full_name"]).replace(' ', '')
                    tax_number = safe_string(row["tax_number"]).replace(' ', '')
                    if member_name == "" or tax_number == "":
                        continue
                    #106  1409 2189
                    sql = "SELECT * FROM " + self.__new_table_name + " WHERE company_full_name = %s";
                    db.cursor.execute(sql, (row["company_full_name"]))
                    members = db.cursor.fetchall();
                    if len(members) > 0:
                        continue

                    sql = "INSERT INTO " + self.__new_table_name + "  VALUES(%s, %s, %s, %s, %s, %s\
                    ,%s, %s, %s, %s, %s)"

                    param = (row["id"],row["company_full_name"], safe_string(row["company_short_name"]), py.get_pinyin(safe_string(row["company_full_name"])).replace("-","")\
                            ,py.get_pinyin(safe_string(row["company_short_name"])).replace("-",""),row["tax_number"] \
                            ,"0", row["create_time"], time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(row["create_time"])/1000)) \
                            ,"1", row["last_access"] )

                    db.cursor.execute(sql, param)
                    db.conn.commit()


                    sql = "INSERT INTO t_member_public(member_id,company_nature,company_type,province_code\
                            , province_name, city_code, city_name, district_code, district_name\
                            , address, reg_time, reg_capital, register_money_type, legal_person_name\
                            ,legal_person_id_card, legal_person_phone, linkman, mobile, phone, fax\
                            , post_code, email, website, `scale`, remark,editor_id, last_access, creator_id\
                            , create_time, version)  VALUES(%s, %s, %s, %s, %s, %s\
                                ,%s, %s, %s, %s, %s, %s \
                                ,%s, %s, %s, %s, %s, %s \
                                ,%s, %s, %s, %s, %s, %s \
                                ,%s, %s, %s, %s, %s, %s \
                            )"

                    param = (row["id"],row["company_nature"], row["company_type"], safe_string(row["province_code"])\
                            ,safe_string(row["province_name"]), safe_string(row["city_code"]), safe_string(row["city_name"]), \
                             safe_string(row["district_code"]), safe_string(row["district_name"]), safe_string(row["address"]) \
                            ,safe_string(row["reg_time"]),safe_string(row["reg_capital"]), row["register_money_type"],\
                             safe_string(row["legal_person_name"]),safe_string(row["legal_person_id_card"]), row["legal_person_phone"],\
                             safe_string(row["linkman"]),safe_string(row["mobile"]), safe_string(row["phone"]),safe_string(row["fax"]),safe_string(row["post_code"]),\
                             safe_string(row["email"]),safe_string(row["website"]), safe_string(row["scale"]),safe_string(row["remark"]),row["editor_id"],\
                            row["last_access"], row["creator_id"],  row["create_time"]\
                            ,"1")

                    db.cursor.execute(sql, param)
                    db.conn.commit()
                    #打印结果
                    print(param)
            except:
                traceback.print_exc()
    pass

    # 会员公共信息
    # def member_public_process(self):
    #     with self.conn as db:
    #         sql = "SELECT * FROM  member" ;
    #         try:
    #             # 获取所有记录列表
    #             db.cursor.execute(sql)
    #             results = db.cursor.fetchall();
    #             for row in results:
    #                 member_name = safe_string(row["company_full_name"]).replace(' ', '')
    #                 tax_number = safe_string(row["tax_number"]).replace(' ', '')
    #                 if member_name == '' or tax_number == '':
    #                     continue
    #                 #去重
    #                 db.cursor.execute("SELECT id FROM t_member_public WHERE member_id = %s", (row["id"]))
    #                 members = db.cursor.fetchall();
    #                 if len(members) > 0:
    #                     continue
    #
    #                 sql = "INSERT INTO t_member_public(member_id,company_nature,company_type,province_code\
    #                         , province_name, city_code, city_name, district_code, district_name\
    #                         , address, reg_time, reg_capital, register_money_type, legal_person_name\
    #                         ,legal_person_id_card, legal_person_phone, linkman, mobile, phone, fax\
    #                         , post_code, email, website, `scale`, remark,editor_id, last_access, creator_id\
    #                         , create_time, version)  VALUES(%s, %s, %s, %s, %s, %s\
    #                             ,%s, %s, %s, %s, %s, %s \
    #                             ,%s, %s, %s, %s, %s, %s \
    #                             ,%s, %s, %s, %s, %s, %s \
    #                             ,%s, %s, %s, %s, %s, %s \
    #                         )"
    #
    #                 param = (row["id"],row["company_nature"], row["company_type"], safe_string(row["province_code"])\
    #                         ,safe_string(row["province_name"]), safe_string(row["city_code"]), safe_string(row["city_name"]), \
    #                          safe_string(row["district_code"]), safe_string(row["district_name"]), safe_string(row["address"]) \
    #                         ,safe_string(row["reg_time"]),safe_string(row["reg_capital"]), row["register_money_type"],\
    #                          safe_string(row["legal_person_name"]),safe_string(row["legal_person_id_card"]), row["legal_person_phone"],\
    #                          safe_string(row["linkman"]),safe_string(row["mobile"]), safe_string(row["phone"]),safe_string(row["fax"]),safe_string(row["post_code"]),\
    #                          safe_string(row["email"]),safe_string(row["website"]), safe_string(row["scale"]),safe_string(row["remark"]),row["editor_id"],\
    #                         row["last_access"], row["creator_id"],  row["create_time"]\
    #                         ,"1")
    #
    #                 db.cursor.execute(sql, param)
    #                 db.conn.commit()
    #                 #打印结果
    #                 print(param)
    #         except Exception as e:
    #             traceback.print_exc()
    # pass

    def trade_member(self):
        self.__old_table_name = "member_trade_info"
        self.__new_table_name = "t_member_trade"

        with self.conn as db:
            sql = "SELECT * FROM member_trade_info" ;
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    db.cursor.execute("SELECT id FROM t_member_trade WHERE member_id = %s", (row["member_id"]))
                    trade_members = db.cursor.fetchall();
                    if len(trade_members) > 0:
                        continue


                    db.cursor.execute("SELECT `name` FROM employee where id = %s", (row["broker_id"]))
                    broker_name = db.cursor.fetchone();

                    db.cursor.execute("SELECT `name` FROM department where id = %s", (row["admin_dept_id"]))
                    adminDeptName= db.cursor.fetchone();

                    sql = "INSERT INTO " + self.__new_table_name + "  VALUES(%s, %s, %s, %s, %s, %s\
                        ,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)"

                    param = (row["id"], row["member_id"], row["status"], 0, row["last_trade_time"] \
                             ,row["admin_id"], row["admin_dept_id"], safe_string(adminDeptName["name"]), row["broker_id"], safe_string(broker_name["name"]), row["auth_status"] \
                             , row["auth_time"], row["lock_user_id"], row["lock_time"], safe_string(row["lock_reason"]),\
                              row["editor_id"], row["last_access"], row["creator_id"], row["create_time"], "1")

                    db.cursor.execute(sql, param)
                    db.conn.commit()
                    # 打印结果
                    print(param)

                db.cursor.execute("delete from  t_member_trade where member_id in (4,185,351)")
                db.conn.commit()
            except:
                traceback.print_exc()
    pass




def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");

def safe_int(object):
    return  0  if object == None  else object;



if __name__ == '__main__':
    member = MemberProcess(fd.conn)
    member.member_process()
    member.trade_member()