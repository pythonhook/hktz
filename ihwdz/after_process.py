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
        self.tables = ("member_factory_material", "member_factory_plant", "member_factory_product", "member_invoice" \
                       , "member_manage_product", "member_memo", "member_lock_info", "member_visit", "member_visit_images")

    def p_tables(self):
        with self.conn as db:
            try:
                for table in self.tables:
                    # 字段处理
                    sql = "DESC " + "`" + table + "`"
                    db.cursor.execute(sql)
                    cols = db.cursor.fetchall();
                    for col in cols:
                        field = safe_string(col["Field"])
                        if field.__contains__("valid"):
                            db.cursor.execute("DELETE FROM `" + table + "` WHERE valid = 0")
                            db.conn.commit()
                            db.cursor.execute("ALTER TABLE `" + table + "` DROP  COLUMN `valid`")
                        if field.__contains__("editor_id"):
                            db.cursor.execute("ALTER TABLE `" + table + "` DROP  COLUMN `editor_id`")
                        if field.__contains__("version"):
                            db.cursor.execute("update `" + table + "` set version = 1")
                            db.conn.commit()
                            db.cursor.execute("ALTER TABLE `" + table + "` MODIFY `version` INT(4)  DEFAULT 1")

                    temp_table = self.prefix + table
                    db.cursor.execute("DROP TABLE `" + temp_table + "`")
                    db.cursor.execute("ALTER TABLE `" + table + "` RENAME " + temp_table)
            except:
                traceback.print_exc()
    pass


def safe_string(object):
    return  "" if object == None  else object.decode("utf-8");


if __name__ == "__main__":
    tables = Tables(fd.conn)
    tables.p_tables()