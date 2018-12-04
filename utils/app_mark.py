#-*- coding: UTF-8 -*-
import time
import datetime
import core.config.global_var as fd
import xlrd
from xlrd import xldate_as_tuple
import decimal

__conn = fd.conn


# def  calc_future_current():
#     cur_time = int(time.time())
#     cur_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cur_time))
#
#     #lldpe 1 pp 2  pvc 3
#     baseId = 2
#     #lldpe: 5 pp 4  pvc 3
#     bid = 4
#     workbook = xlrd.open_workbook('E:/xlsx/future-current/pp.xls')
#     booksheet = workbook.sheet_by_name('Sheet1')
#
#     for row in range(booksheet.nrows):
#         row_data = []
#         for col in range(booksheet.ncols):
#             cel = booksheet.cell(row, col)
#             val = cel.value
#             try:
#                 if cel.ctype == 3:
#                     date = xldate_as_tuple(val, 0)
#                     val = datetime.datetime(*date).strftime("%Y-%m-%d")
#                 else:
#                     val = str(val).replace(' ', '')
#             except:
#                 pass
#             row_data.append(val)
#
#         print(row_data)
#         future_price = 0.00
#         with __conn as db:
#             #取期货价格
#             db.cursor.execute("select closing_price as price from t_product_futures where base_id = %s and release_date_str=%s",(bid, row_data[0]))
#             future = db.cursor.fetchone()
#             if future == None:
#                 db.cursor.execute("select avg(closing_price) as price from t_product_futures where base_id = %s", (bid))
#                 avg_price = db.cursor.fetchone()
#                 future_price = round(avg_price["price"], 2 )
#             else:
#                 future_price = future["price"]
#
#             diff = future_price - float(row_data[1])
#             diff = int(diff)
#
#
#             sql = "insert into t_product_future_current_item (base_id, current_price, future_price, diff,date_time_str,create_time,create_time_str) \
#                     VALUES(%s, %s, %s, %s,%s, %s, %s) "
#             db.cursor.execute(sql,(baseId, row_data[1], future_price, diff,row_data[0], cur_time, cur_time_str))
#             db.conn.commit()
#成本利润数据
def  calc_cost_profit():
    cur_time = int(time.time())
    cur_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cur_time))

    #pvc 3  pp 2 lldpe 1
    #pvc 14430  pp 14646 lldpe: 14426
    baseId = 2
    bid = 14646
    workbook = xlrd.open_workbook('E:/xlsx/cost-profit/pp.xls')
    booksheet = workbook.sheet_by_name('Sheet1')

    for row in range(booksheet.nrows):
        row_data = []
        for col in range(booksheet.ncols):
            cel = booksheet.cell(row, col)
            val = cel.value
            try:
                if cel.ctype == 3:
                    date = xldate_as_tuple(val, 0)
                    val = datetime.datetime(*date).strftime("%Y-%m-%d")
                else:
                    val = str(val).replace(' ', '')
            except:
                pass
            row_data.append(val)

        print(row_data)
        material_price = 0.00
        with __conn as db:
            #取原料价格
            db.cursor.execute("select * from t_product_price_material where base_id = %s and release_date_str=%s",(bid, row_data[0]))
            material = db.cursor.fetchone()
            if material == None:
                db.cursor.execute("select avg(price) as price from t_product_price_material where base_id = %s", (bid))
                avg_price = db.cursor.fetchone()
                material_price = round(avg_price["price"], 2 )*2.8
            else:
                material_price = material["price"]*2.8

            #上一天价格
            db.cursor.execute("select profit from t_product_cost_profit_item where base_id = %s order by id desc limit 1",(baseId))
            latest_profit = db.cursor.fetchone()
            if latest_profit == None:
                latest_profit = {"profit" : 0.00}

            profit = float(row_data[1]) - material_price
            profit = int(profit)
            rate = profit/float(row_data[1])
            up_down = profit - latest_profit["profit"]

            sql = "insert into t_product_cost_profit_item (base_id, price, material, profit,rate,up_down,date_time_str,create_time,create_time_str) \
                    VALUES(%s, %s, %s, %s,%s, %s, %s, %s, %s) "
            db.cursor.execute(sql,(baseId, row_data[1], material_price, profit, rate, up_down,row_data[0], cur_time, cur_time_str))
            db.conn.commit()

if __name__ == '__main__':
    calc_cost_profit()

