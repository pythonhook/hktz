from datetime import datetime,date
import time
#当前时间周序号
#print(datetime.datetime.now().isocalendar())
#
#print(datetime.date(2014, 6, 8).isocalendar()[1])

#print(datetime.today().weekday() + 1)
x = time.localtime(1516867472)
print(time.strftime('%Y-%m-%d %H:%M:%S', x))

print(int(time.mktime(time.strptime("2018-10-17 16:18:29",'%Y-%m-%d %H:%M:%S'))))
print(int(time.mktime(time.strptime("2018-10-17 16:14:46",'%Y-%m-%d %H:%M:%S'))))
print(int(time.mktime(time.strptime("2018-10-17 16:07:41",'%Y-%m-%d %H:%M:%S'))))

