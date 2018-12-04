#-*- coding: UTF-8 -*-

import  traceback
import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import core.config.global_var as fd
from xpinyin import Pinyin
from utils.logging import Log

log = Log().getInstance("MemberRiskAccountProcess")

#会员信息
class MemberItemValueProcess(object):

    def __init__(self, conn):
        self.conn = conn


    def member_item_value_process(self):
        cur_time = int(time.time())
        cur_short_time = time.strftime('%Y-%m-%d', time.localtime(cur_time))
        cur_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cur_time))

        with self.conn as db:
            sql = "SELECT * FROM finance_item_value" ;
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    if row["apply_id"] == None:
                        continue
                    # 申请是否存在，不存在continue
                    db.cursor.execute("SELECT member_id, member_name FROM finance_amount_apply WHERE id = %s ", (row["apply_id"]))
                    applies = db.cursor.fetchall();
                    if len(applies) == 0:
                        d = dict()
                        d["finance_item_value_id"] = row["id"]
                        d["apply_id"] = row["apply_id"]
                        d["msg"] = "finance_item_value--对应申请不存在"
                        log.info(d)
                        continue

                    member_id = applies[0]["member_id"]
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s ", (member_id))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        d = dict()
                        d["finance_item_value_id"] = row["id"]
                        d["apply_id"] = row["apply_id"]
                        d["member_id"] = member_id
                        d["msg"] = "finance_item_value--申请对应的会员不存在"
                        log.info(d)
                        continue

                    # 去重
                    db.cursor.execute("SELECT id FROM t_member_item_value WHERE member_id = %s AND group_id = %s ", (member_id, row["group_id"]))
                    items = db.cursor.fetchall();
                    if len(items) > 0:
                        continue
                    # 打分项
                    popName = None
                    db.cursor.execute("SELECT `prop_name` FROM t_risk_grade_group where id = %s ", (row["group_id"]))
                    popName = db.cursor.fetchone()
                    if popName == None:
                        continue
                    #新的分数
                    scoreMap = None
                    db.cursor.execute("SELECT `item_value` FROM t_risk_grade_item where id = %s ", (row["item_id"]))
                    scoreMap = db.cursor.fetchone()
                    if scoreMap == None:
                        continue

                    sql = "INSERT INTO t_member_item_value  VALUES(%s, %s, %s, %s, %s, %s \
                    ,%s, %s, %s, %s, %s, %s, %s)"

                    mkt = int(time.mktime(time.strptime(safe_string(row["created_time"]), '%Y-%m-%d %H:%M:%S'))) * 1000
                    param = (row["id"], member_id, row["group_id"], row["item_id"], safe_string(popName["prop_name"])\
                            , safe_string(scoreMap["item_value"]), safe_string(row["item_value_ext"]) \
                             , safe_string(row["created_by_name"]), row["created_by"], \
                             mkt, safe_string(row["created_time"]) , "1", mkt)

                    db.cursor.execute(sql, param)
                    db.conn.commit()
                    #打印结果
                    print(param)
            except Exception as e:
                traceback.print_exc()
    pass



def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");
def safe_string2(object):
    return  "0" if object == None  else object.decode("utf-8");

if __name__ == '__main__':
    member = MemberItemValueProcess(fd.conn)
    member.member_item_value_process()
