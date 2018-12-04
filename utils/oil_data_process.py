#-*- coding: UTF-8 -*-

import datetime
import time
from core.config.connection_pool import ConnectionPool;
import  sys


class OilProcess(object):

    def __init__(self, conn):
        self.conn = conn

    """
       表名table : t_oil_sub_sales_stats
       开始id : from_zero  默认从 id = 0 开始更新 ， 非None 从最大id开始更新 
    """
    def oil_sub_sales_stats_process(self, from_zero = None):
        with self.conn as db:
            # 计算当前最大id
            max_id = None
            # 计算当前最大id
            if from_zero == None:
                max_id = 0
            else:
                max_id = db.cursor.execute("SELECT MAX(id) FROM t_oil_sub_sales_stats")
                max_id = db.cursor.fetchone()[0]
                print("max_id:%s" % max_id)

            # SQL 查询语句;
            sql = "SELECT * FROM t_oil_sub_sales_stats where id > '" + str(max_id) + "'";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    id = row[0]
                    date_time = row[6].decode("utf-8")
                    if "-" in date_time:
                        dts = date_time.split("-")
                        sql = "UPDATE t_oil_sub_sales_stats set year = %s , month = %s  where id = %s"
                        param = (dts[0], dts[1], id)

                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        # 打印结果
                        print(sql)

            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
        pass

    """
       表名table : t_oil_sub_yield_stats
       开始id : from_zero  默认从 id = 0 开始更新 ， 非None 从最大id开始更新 
    """
    def oil_sub_yield_stats_process(self, from_zero = None):
        with self.conn as db:
            max_id = None
            # 计算当前最大id
            if from_zero == None:
                max_id = 0
            else:
                max_id = db.cursor.execute("SELECT MAX(id) FROM t_oil_sub_yield_stats")
                max_id = db.cursor.fetchone()[0]
                print("max_id:%s" % max_id)
            # SQL 查询语句;
            sql = "SELECT * FROM t_oil_sub_yield_stats where id > '" + str(max_id) + "'";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    id = row[0]
                    date_time = row[6].decode("utf-8")
                    if "-" in date_time:
                        dts = date_time.split("-")
                        sql = "UPDATE t_oil_sub_yield_stats set year = %s , month = %s  where id = %s"
                        param = (dts[0], dts[1], id)

                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        # 打印结果
                        print(sql)

            except:
                print(sys.exc_info()[0], sys.exc_info()[1])
        pass

    """
       原油库存 : t_oil_stock_stats
       开始id :  from_zero  默认从 id = 0 开始更新 ， 非None 从最大id开始更新  
    """
    def oil_stock_stats_process(self, from_zero = None):
        with self.conn as db:
            max_id = None
            # 计算当前最大id
            if from_zero == None:
                max_id = 0
            else:
                max_id = db.cursor.execute("SELECT MAX(id) FROM t_oil_stock_stats")
                max_id = db.cursor.fetchone()[0]
                print("max_is : %s" % max_id)

            # SQL 查询语句;
            sql = "SELECT * FROM t_oil_stock_stats where id > '" + str(max_id) + "'";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    id = row[0]
                    date_time = row[4].decode("utf-8")

                    if "-" in date_time:
                        dts = date_time.split("-")
                        sql = "UPDATE t_oil_stock_stats SET year = %s , month = %s, week = %s, day = %s WHERE id = %s"
                        #计算周序列
                        week = datetime.date(int(dts[0]), int(dts[1]), int(dts[2])).isocalendar()[1]
                        param = (dts[0], dts[1], str(week), dts[2], str(id))
                        #print(sql)
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        # 打印结果
                        print(sql)

            except :
                print(sys.exc_info()[0], sys.exc_info()[1])
        pass


    """
       进口数据 : t_oil_import_stats
       开始id :  from_zero  默认从 id = 0 开始更新 ， 非None 从最大id开始更新  
    """
    def oil_import_stats_process(self, from_zero = None):
        with self.conn as db:
            max_id = None
            # 计算当前最大id
            if from_zero == None:
                max_id = 0
            else:
                max_id = db.cursor.execute("SELECT MAX(id) FROM t_oil_import_stats")
                max_id = db.cursor.fetchone()[0]
                print("max_is : %s" % max_id)

            # SQL 查询语句;
            sql = "SELECT * FROM t_oil_import_stats where id > '" + str(max_id) + "'";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    id = row[0]
                    date_time = row[5].decode("utf-8")

                    if "-" in date_time:
                        dts = date_time.split("-")
                        sql = "UPDATE t_oil_import_stats SET year = %s , month = %s  WHERE id = %s"
                        param = (dts[0], dts[1], str(id))
                        #print(sql)
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        # 打印结果
                        print(sql)

            except :
                print(sys.exc_info()[0], sys.exc_info()[1])
        pass


    """
       进口数据 : t_oil_export_stats
       开始id :  from_zero  默认从 id = 0 开始更新 ， 非None 从最大id开始更新  
    """
    def oil_export_stats_process(self, from_zero = None):
        with self.conn as db:
            max_id = None
            # 计算当前最大id
            if from_zero == None:
                max_id = 0
            else:
                max_id = db.cursor.execute("SELECT MAX(id) FROM t_oil_export_stats")
                max_id = db.cursor.fetchone()[0]
                print("max_is : %s" % max_id)

            # SQL 查询语句;
            sql = "SELECT * FROM t_oil_export_stats where id > '" + str(max_id) + "'";
            try:
                # 获取所有记录列表
                db.cursor.execute(sql)
                results = db.cursor.fetchall();
                for row in results:
                    id = row[0]
                    date_time = row[5].decode("utf-8")

                    if "-" in date_time:
                        dts = date_time.split("-")
                        sql = "UPDATE t_oil_export_stats SET year = %s , month = %s  WHERE id = %s"
                        param = (dts[0], dts[1], str(id))
                        #print(sql)
                        db.cursor.execute(sql, param)
                        db.conn.commit()
                        # 打印结果
                        print(sql)

            except :
                print(sys.exc_info()[0], sys.exc_info()[1])
        pass