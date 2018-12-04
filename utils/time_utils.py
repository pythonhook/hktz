#-*- coding: UTF-8 -*-
import time
from datetime import datetime,date
import core.config.global_var as fd


class TimeUtils(object):
    __conn = None

    # 当前是不是假期
    def is_holiday(self):
        holidays = []
        if self.__conn == None:
            self.__conn = fd.conn
            with self.__conn as db:
                t = int(time.time())
                db.cursor.execute("SELECT id,name,day_num FROM ihwdz_system_holiday where  day_start < %s AND day_end > %s and  type = 1", (t, t))
                holidays = db.cursor.fetchall()

        return  len(holidays) > 0

    #当前是不是周末
    def is_week_end(self):
        week_day = datetime.today().weekday() + 1
        return (week_day == 6) or (week_day == 7)

    def gmt2str(self, gmt):
        x = time.localtime(gmt)
        return time.strftime('%Y-%m-%d %H:%M:%S', x)