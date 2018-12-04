#-*- coding: UTF-8 -*-

import traceback
import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import os
import core.config.global_var as fd

# 追加mysql的命令到环境变量
sys.path.append("D:\mysql")

class Tables(object):

    def __init__(self, conn):
        self.conn = conn
        self.prefix = "t_"
        self.backup = "D:/mysql/backup"

    def modify_tables(self):
        with self.conn as db:
            sql = "show tables"
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    tbl_name = safe_string(row["Tables_in_test"])

                    if tbl_name == 'v' :
                        break
                    # 处理数据表中的 datetime->vachar(30) 和 decimal->double(20, 2)类型
                    sql = "DESC " + "`" + tbl_name + "`"
                    print(sql)
                    db.cursor.execute(sql)
                    cols = db.cursor.fetchall();
                    for col in cols:
                        field = safe_string(col["Field"])
                        type = safe_string(col["Type"])
                        if type.__contains__("datetime"):
                            sql = "ALTER TABLE `" + tbl_name + "` MODIFY " + field + " VARCHAR(50)  "
                        if type.__contains__("decimal"):
                            sql = "ALTER TABLE `" + tbl_name + "` MODIFY " + field + " DOUBLE(20, 4)  DEFAULT NULL"
                        db.cursor.execute(sql)
                    # 修改表名
                    if tbl_name.startswith(self.prefix):
                        new_tbl_name = tbl_name.lstrip(self.prefix)
                        db.cursor.execute("ALTER TABLE `" + tbl_name + "` RENAME " + new_tbl_name)
            except:
                traceback.print_exc()
    pass

    def mysql_dump_member(self):
        if not os.path.exists(self.backup + "/member"):
            os.makedirs(self.backup+ "/member")
        os.chdir(self.backup + "/member")

        mysqlcomm = 'mysqldump'
        dbserver = '172.16.10.43'
        dbuser = 'root'
        dbpasswd = 'aokai100'
        dbname = 'test'
        dbtable = 't_member t_member_apply_log t_member_attachment t_member_common t_member_common_apply t_member_delivery_address' + \
                  ' t_member_factory_device t_member_factory_material t_member_factory_plant t_member_factory_product t_member_invoice' + \
                  ' t_member_lock_info t_member_manage_product t_member_memo t_member_payment t_member_payment_invoice t_member_public' + \
                  ' t_member_rank t_member_sell t_member_sell_apply t_member_situation_description t_member_trade t_member_trade_apply ' + \
                  ' t_member_trade_transfer_log t_member_visit t_member_visit_images t_user t_user_apply t_user_log'
        exportfile = 'member.sql'

        sqlfromat = "%s -h%s -u%s -p%s  %s %s >%s"
        sql = (sqlfromat % (mysqlcomm, dbserver, dbuser, dbpasswd, dbname, dbtable, exportfile))
        print(sql)
        #os.system(sql)
        # result = os.system(sql)
        # if result:
        #     print("backup success!")
        # else:
        #     print("backup failed!")
    pass

    def mysql_dump_risk(self):
        if not os.path.exists(self.backup + "/risk"):
            os.makedirs(self.backup+ "/risk")
        os.chdir(self.backup + "/risk")

        mysqlcomm = 'mysqldump'
        dbserver = '172.16.10.43'
        dbuser = 'root'
        dbpasswd = 'aokai100'
        dbname = 'test'
        dbtable = 't_member_affiliate_info t_member_chattel_mortgage t_member_credit_score t_member_external_guarantee ' + \
                  't_member_grade_collect t_member_item_value t_member_risk_account t_member_risk_account_log t_member_risk_rank' + \
                  't_member_score_rank t_member_shareholder t_member_susong_situation t_member_validation t_risk_grade_group t_risk_grade_item'
        exportfile = 'risk.sql'

        sqlfromat = "%s -h%s -u%s -p%s  %s %s >%s"
        sql = (sqlfromat % (mysqlcomm, dbserver, dbuser, dbpasswd, dbname, dbtable, exportfile))
        print(sql)
        #os.system(sql)
        # result = os.system(sql)
        # if result:
        #     print("backup success!")
        # else:
        #     print("backup failed!")
    pass

def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");


if __name__ == "__main__":
    tables = Tables(fd.conn)
    tables.modify_tables()
