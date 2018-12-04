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
class CompanyProcess(object):

    def __init__(self, conn):
        self.conn = conn

    # 会员名称
    def company_process(self):
        with self.conn as db:
            sql = "delete from  company where id > 352713";
            db.cursor.execute(sql)
            db.conn.commit()


            sql = "SELECT a.id, b.id as member_id, b.company_full_name as member_name FROM t_member_trade as a left join \
                   t_member as b on a.member_id = b.id ";
            try:
                # 获取所有交易会员列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    sql = "select id from company where company_name = %s"
                    db.cursor.execute(sql, (safe_string(row["member_name"])))
                    companies = db.cursor.fetchall();
                    if len(companies) > 0:
                        continue

                    sql = "insert into company(member_id, company_name) values(%s, %s)"
                    param = (row["member_id"], safe_string(row["member_name"]))
                    db.cursor.execute(sql, param)
                    db.conn.commit()
                    print(param)
            except Exception as e:
                    traceback.print_exc()
    pass



def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");


if __name__ == '__main__':
    company = CompanyProcess(fd.conn)
    company.company_process()