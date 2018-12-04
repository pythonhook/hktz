#-*- coding: UTF-8 -*-

import traceback
import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys
import core.config.global_var as fd
from xpinyin import Pinyin

#会员信息
class MemberAttachmentProcess(object):

    def __init__(self, conn):
        self.conn = conn
        self.__old_table_name="member";
        self.__new_table_name="t_member";

    # 会员-认证附件old->member_attachment    new->t_member_attachment
    def attachment_process(self):
        with self.conn as db:
            sql = "SELECT * FROM member_attachment where valid = 1";
            try:
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    # 会员是否存在，不存在continue
                    db.cursor.execute("SELECT id FROM t_member WHERE id = %s", (row["member_id"]))
                    members = db.cursor.fetchall();
                    if len(members) == 0:
                        continue

                    sql = "INSERT INTO t_member_attachment(member_id, attachment_type, attachment_path,\
                            attachment_name,creator_id,create_time,version, last_access) \
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"

                    param = (members[0]["id"], row["attachment_type"], safe_string(row["attachment_path"]),
                             safe_string(row["attachment_name"]) \
                                 , row["creator_id"], row["create_time"], "1", row["last_access"])
                    print(param)
                    db.cursor.execute(sql, param)
                    db.conn.commit()
            except:
                traceback.print_exc()

    pass


def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");


if __name__ == '__main__':
    member = MemberAttachmentProcess(fd.conn)
    member.attachment_process()
