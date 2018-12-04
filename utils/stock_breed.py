# -*- coding: UTF-8 -*-
import core.config.global_var as fd
import time
import traceback
from datetime import datetime, date
from utils.logging import Log


#日志
log = Log().getInstance("stock_breed")

class StockBreed(object):
    __conn = None
    __breeds = None

    def get_breeds(self):
        if self.__conn == None:
            self.__conn = fd.conn
            with self.__conn as db:
                db.cursor.execute("SELECT * FROM ihwdz_system_breed where parent_id > 0")
                breeds = db.cursor.fetchall()
                self.__breeds = []
                for row in breeds:
                    self.__breeds.append(
                        {"id": str(row["id"]), "parent_id": row["parent_id"], "name": row["breed_name"].decode("utf-8"),
                         "breed_code": row["breed_code"].decode("utf-8"),
                         "out_code": row["breed_out_id"].decode("utf-8")})

        return self.__breeds

    # 更新今日仓位状态
    def update_position(self):
        cur_time = int(time.time())
        cur_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cur_time))
        if self.__conn == None:
            self.__conn = fd.conn
        else:
            with self.__conn as db:
                positions = None
                try:
                    db.cursor.execute("select * from ihwdz_reporter_position where effective_start < %s AND effective_end > %s  AND  is_out_date = 0",(cur_time, cur_time))
                    positions = db.cursor.fetchall()
                except Exception as e:
                    log.error(e)
                    return
                if positions == None:
                    return
                log.info("************update_position*********** SART")
                for row in positions:
                    try:
                        if row["breed_code"] == None:
                            continue

                        stock_Breed_Code = row["breed_code"].decode("utf-8")
                        if stock_Breed_Code.strip() not in fd.gl_future_dict:
                            continue

                        stock_info = fd.gl_future_dict.get(stock_Breed_Code.strip())
                        if int(time.mktime(time.strptime(stock_info["trading_date"], '%Y-%m-%d %H:%M:%S'))) < row["effective_start"]:
                            continue

                        trading_date = stock_info["trading_date"]
                        max_price = float(stock_info["max_price"]) if stock_info["max_price"] != None and stock_info["max_price"] != "-" else 0
                        min_price = float(stock_info["min_price"]) if stock_info["min_price"] != None and stock_info["min_price"] != "-" else 0
                        open_price = float(stock_info["open_price"]) if stock_info["open_price"] != None and stock_info["open_price"] != "-" else 0
                        cur_price = float(stock_info["cur_price"]) if stock_info["cur_price"] != None and stock_info["cur_price"] != "-" else 0
                        yes_close_price = float(stock_info["yes_close_price"]) if stock_info["yes_close_price"] != None and \
                                                                                  stock_info["yes_close_price"] != "-" else 0
                        rate = stock_info["rate"] if stock_info["rate"] != None and stock_info["rate"] != "-" else 0
                        # 开仓
                        if row["position_status"] == 0 and row["open_price"] > 0 and row["stop_win_price"] > 0 and row["stop_loss_price"] > 0:
                            # 看多
                            if row["long_or_short"] == 1:
                                if row["open_price"] >= min_price:
                                    db.cursor.execute("update ihwdz_reporter_position set position_status = 1,version=version+1,last_access=%s \
                                                       where id = %s and version=%s", (cur_time, row["id"], row["version"]));
                                    db.conn.commit()
                            # 看空
                            if row["long_or_short"] == 2:
                                if row["open_price"] <= max_price:
                                    db.cursor.execute("update ihwdz_reporter_position set position_status = 1,version=version+1,last_access=%s \
                                                       where id = %s and version=%s", (cur_time, row["id"], row["version"]));
                                    db.conn.commit()
                            # 观望
                            if row["long_or_short"] == 0:
                                if row["open_price"] > row["stop_loss_price"] and row["open_price"] < row["stop_win_price"]:  # 观望-多
                                    if row["open_price"] >= min_price:
                                        db.cursor.execute("update ihwdz_reporter_position set position_status = 1,version=version+1,last_access=%s \
                                                           where id = %s and version=%s", (cur_time, row["id"], row["version"]));
                                        db.conn.commit()
                                if row["open_price"] > row["stop_win_price"] and row["open_price"] < row[ "stop_loss_price"]:  # 观望-空
                                    if row["open_price"] <= max_price:
                                        db.cursor.execute("update ihwdz_reporter_position set position_status = 1,version=version+1,last_access=%s \
                                                           where id = %s and version=%s", (cur_time, row["id"], row["version"]));
                                        db.conn.commit()

                        if row["position_status"] == 1:
                            if row["long_or_short"] == 1:  # 看多
                                if max_price >= row["stop_win_price"]:  # 止盈平仓
                                    db.cursor.execute("update ihwdz_reporter_position set position_status = 13, close_time = %s, close_time_str= %s,version=version+1,last_access=%s\
                                                        where id = %s and version=%s", (cur_time, cur_time_str, cur_time, row["id"], row["version"]));
                                    db.conn.commit()
                                if min_price <= row["stop_loss_price"]:  # 止损平仓
                                    db.cursor.execute("update ihwdz_reporter_position set position_status = 12, close_time = %s, close_time_str= %s,version=version+1,last_access=%s \
                                                       where id = %s and version=%s ", (cur_time, cur_time_str, cur_time, row["id"], row["version"]));
                                    db.conn.commit()
                            if row["long_or_short"] == 2:  # 看空
                                if min_price <= row["stop_win_price"]:  # 止盈平仓
                                    db.cursor.execute("update ihwdz_reporter_position set position_status = 23, close_time = %s, close_time_str= %s,version=version+1,last_access=%s \
                                                       where id = %s and version=%s", (cur_time, cur_time_str, cur_time, row["id"], row["version"]));
                                    db.conn.commit()

                                if max_price >= row["stop_loss_price"]:  # 止损平仓
                                    db.cursor.execute("update ihwdz_reporter_position set position_status = 22,close_time = %s,close_time_str=%s,version=version+1,last_access=%s \
                                                       where id = %s and version=%s", (cur_time, cur_time_str, cur_time, row["id"], row["version"]));
                                    db.conn.commit()

                            if row["long_or_short"] == 0:  # 观望
                                if row["open_price"] > row["stop_loss_price"] and row["open_price"] < row[
                                    "stop_win_price"]:  # 看多
                                    if max_price >= row["stop_win_price"]:  # 止盈平仓
                                        db.cursor.execute("update ihwdz_reporter_position set position_status = 13, close_time = %s, close_time_str= %s,version=version+1,last_access=%s\
                                                           where id = %s and version=%s", (cur_time, cur_time_str, cur_time, row["id"], row["version"]));
                                        db.conn.commit()
                                    if min_price <= row["stop_loss_price"]:  # 止损平仓
                                        db.cursor.execute("update ihwdz_reporter_position set position_status = 12, close_time = %s, close_time_str= %s,version=version+1,last_access=%s \
                                                           where id = %s and version=%s ", (cur_time, cur_time_str, cur_time, row["id"], row["version"]));
                                        db.conn.commit()
                                if row["open_price"] > row["stop_win_price"] and row["open_price"] < row[
                                    "stop_loss_price"]:  # 看空
                                    if min_price <= row["stop_win_price"]:  # 止盈平仓
                                        db.cursor.execute("update ihwdz_reporter_position set position_status = 23, close_time = %s, close_time_str= %s,version=version+1,last_access=%s \
                                                            where id = %s and version=%s", (cur_time, cur_time_str, cur_time, row["id"], row["version"]));
                                        db.conn.commit()
                                    if max_price >= row["stop_loss_price"]:  # 止损平仓
                                        db.cursor.execute("update ihwdz_reporter_position set position_status = 22,close_time = %s,close_time_str=%s,version=version+1,last_access=%s \
                                                               where id = %s and version=%s",(cur_time, cur_time_str, cur_time, row["id"], row["version"]));
                                        db.conn.commit()
                        # 在有效期内把价格数据同步到数据表
                        if max_price > 0:
                            db.cursor.execute("update ihwdz_reporter_position set max_price=%s,min_price=%s,cur_price=%s,tod_open_price=%s,yes_close_price=%s,rate=%s, version = version +1, last_access =%s where id = %s and version = %s",
                                (max_price, min_price, cur_price, open_price, yes_close_price, rate, cur_time, row["id"],
                                 row["version"]));
                            db.conn.commit()

                            db.cursor.execute(
                                "INSERT INTO ihwdz_date_record ( reporter_position_id,breed_name,breed_code,trading_date,max_price,min_price,cur_price,tod_open_price,yes_close_price,rate,create_time ,create_time_str) \
                                 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", \
                                (row["id"], row["breed_name"].decode("utf-8"), row["breed_code"].decode("utf-8"), trading_date, \
                                 max_price, min_price, cur_price, open_price, yes_close_price, rate, cur_time, cur_time_str));
                            db.conn.commit()

                    except Exception as errorMsg:
                        log.error(errorMsg)
                        continue
                log.info("************update_position*********** END")
        pass


    # 更新今日品种的得分
    def update_score(self):
        cur_time = int(time.time())
        cur_short_time = time.strftime('%Y-%m-%d', time.localtime(cur_time))
        cur_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cur_time))

        if self.__conn == None:
            self.__conn = fd.conn
        else:
            with self.__conn as db:
                positions = None
                try:
                    db.cursor.execute( "select s1.* , s2.parent_id from ihwdz_reporter_position AS s1 LEFT JOIN ihwdz_system_breed AS s2 on s1.breed_code = s2.breed_code where s1.effective_end < %s and s1.is_out_date = 0",(cur_time))
                    positions = db.cursor.fetchall()
                except Exception as e:
                    log.error(e)
                    return

                if positions == None:
                    return
                log.info("************update_score*********** SART")
                for row in positions:
                    if row["parent_id"] == 2 or row["parent_id"] == 5:
                        adj_rate = 0.005  # 农产品
                    else:
                        adj_rate = 0.008  # 工业品
                    score = 0
                    # 多空校验
                    if row["long_or_short"] == 0:  # 观望
                        if row["cur_price"] * (1 - adj_rate) <= row["yes_close_price"] and row["cur_price"] * (1 + adj_rate) >= row["yes_close_price"]:
                            score += 2
                    if row["long_or_short"] == 1:  # 看多
                        if row["cur_price"] > row["yes_close_price"]:
                            score += 2
                    if row["long_or_short"] == 2:  # 看空
                        if row["cur_price"] < row["yes_close_price"]:
                            score += 2

                    # 仓位盈利加分
                    if row["position_status"] == 13 or row["position_status"] == 23:  # 止盈平仓
                        score += 1
                    # if row["position_status"] == 12 or row["position_status"] == 22:  # 止损平仓   不处理
                    # score += 1

                    if row["long_or_short"] == 1 and row["position_status"] == 1:  # 看多并且 开仓成功
                        if row["cur_price"] >= row["open_price"]:
                            score += 1

                    if row["long_or_short"] == 2 and row["position_status"] == 1:  # 看空并且 开仓成功
                        if row["cur_price"] <= row["open_price"]:
                            score += 1

                    if row["long_or_short"] == 0 and row["position_status"] == 1:  # 观望并且开仓成功
                        if row["open_price"] > row["stop_loss_price"] and row["open_price"] < row["stop_win_price"]:
                            if row["cur_price"] >= row["open_price"]:
                                score += 1
                        if row["open_price"] > row["stop_win_price"] and row["open_price"] < row["stop_loss_price"]:
                            if row["cur_price"] <= row["open_price"]:
                                score += 1

                    # 支撑位计算加分
                    min_cail_price = row["min_price"] * (1 - 0.005)
                    max_cail_price = row["min_price"] * (1 + 0.005)
                    if row["ceil_price"] >= min_cail_price and row["ceil_price"] <= max_cail_price:
                        score += 1

                    # 阻力位计算
                    min_floor_price = row["max_price"] * (1 - 0.005)
                    max_floor_price = row["max_price"] * (1 + 0.005)
                    if row["floor_price"] >= min_floor_price and row["floor_price"] <= max_floor_price:
                        score += 1
                    if score == 0:
                        score = 1
                    try:
                        # 更新
                        db.cursor.execute("update ihwdz_reporter_position set score = %s,version=version+1 and last_access = %s where id = %s and version = %s ", (score, cur_time, row["id"], row["version"]));
                        db.conn.commit()
                    except Exception as errorMsg:
                        log.error(errorMsg)
                        continue
                log.info("************update_score*********** END")
        pass

    # 更新用户每天的得分
    def update_reporter_score(self):
        cur_time = int(time.time())
        cur_short_time = time.strftime('%Y-%m-%d', time.localtime(cur_time))
        cur_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cur_time))

        if self.__conn == None:
            self.__conn = fd.conn
        else:
            with self.__conn as db:
                rows = None
                try:
                    db.cursor.execute("select id, reporter_id, MAX(score) as score ,score_time from ihwdz_reporter_position where  effective_end < %s and is_out_date = 0  GROUP  BY reporter_id,score_time", \
                        (cur_time))
                    rows = db.cursor.fetchall()
                except Exception as e:
                    log.error(e)
                    return
                log.info("************update_reporter_score*********** SART")
                if len(rows) > 0:
                    for row in rows:
                        sql = "select id from ihwdz_reporter_day_score where reporter_id = %s AND date_time_str = %s AND position_id = %s"
                        db.cursor.execute(sql, (row["reporter_id"], row["score_time"], row["id"]))
                        scores = db.cursor.fetchall()
                        if len(scores) > 0:
                            continue
                        try:
                            db.cursor.execute("INSERT INTO ihwdz_reporter_day_score (reporter_id, position_id, score, date_time_str, create_time, create_time_str)  VALUES(%s, %s, %s, %s, %s, %s)", \
                                (row["reporter_id"], row["id"], row["score"], row["score_time"], cur_time, cur_time_str))
                            db.conn.commit()

                            db.cursor.execute("update ihwdz_reporter_position set  is_out_date = 1  where id = %s", (row["id"]))
                            db.conn.commit()
                        except Exception as errorMsg:
                            continue
                            log.error(errorMsg)
                log.info("************update_reporter_score*********** END")
        pass

