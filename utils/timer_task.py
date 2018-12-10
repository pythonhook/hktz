#-*- coding: UTF-8 -*-

import requests
import threading
import core.config.global_var as fd
from utils.stock_breed import StockBreed
from utils.time_utils import TimeUtils
from utils.logging import Log

lock = threading.Lock()
# 取系统品种
sb = StockBreed()
# 时间
tu = TimeUtils()
#日志
log = Log().getInstance("timer_task")

#运算线程 10秒执行一次
def update_position_status():
    lock.acquire()
    try:
        log.info("更新仓位状态：" + "START")
        sb.update_position()
        log.info("更新仓位状态：" + "END")
    except Exception as e:
        log.error(e)
        lock.release()
    lock.release()

#运算线程2  50分钟执行一次
def update_admin_score():
    lock.acquire()
    try:
        log.info("统计每个仓位的分值：" + "START")
        sb.update_score()
        log.info("统计每个仓位的分值：" + "END")
        log.info("统计 研究员的当天分数 按仓位（品种）平均分：" + "START")
        sb.update_reporter_score()
        log.info("统计 研究员的当天分数 按仓位（品种）平均分：" + "END")
    except Exception as e:
        log.error(e)
        lock.release()
    lock.release()


#设置全局 行情数据
def stock_info():
    lock.acquire()
    try:
        fd.gl_future_dict.clear()
        # 取系统品种
        breeds = sb.get_breeds()
        if len(breeds) > 0:
            log.info("东方财富数据更新：" + "START")
            for s in breeds:
                r = requests.get('http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=' + s['out_code'] + '&sty=FDPBPFBTA&token=7bc05d0d4c3c22ef9fca8c2a912d779c')
                text = r.text;
                text = text.replace("(", "").replace(")", "").replace("]", "").replace("[", "").replace("\"", "").split(",");
                if len(text) > 5:
                    temp = dict()
                    temp["code"] = text[1];
                    temp["name"] = text[2];
                    temp["cur_price"] = text[4];
                    temp["rate"] = text[5];
                    temp["open_price"] = text[7];
                    temp["max_price"] = text[8];
                    temp["yes_close_price"] = text[13];
                    temp["min_price"] = text[14];
                    temp["trading_date"] = text[3] if text[3] != None else "";
                    fd.gl_future_dict[s["breed_code"]] = temp
            log.info("东方财富数据更新：" + "END")
    except Exception as e:
        log.error(e)
        lock.release()
    lock.release()






