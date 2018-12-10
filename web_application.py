#-*- coding: UTF-8 -*-

import core.config.global_var as fd
import schedule
import threading
import time
from flask import Flask, jsonify, request
from utils.timer_task import update_position_status, update_admin_score, stock_info

# 实例化APP
app = Flask(__name__)

def s_jobs():
    #东方财富数据更新
    schedule.every(10).seconds.do(stock_info)
    #更新仓位
    schedule.every(15).seconds.do(update_position_status)
    # 打分
    schedule.every(50).minutes.do(update_admin_score)
    while True:
        schedule.run_pending()
        time.sleep(1)


@app.route('/api/index', methods=['GET'])
def index():
    message = ""
    error = ""
    code = request.values.get("code")

    response = {
        "info" :  fd.gl_future_dict.get(code),
        "message" : message,
        "error"   : error,
        "code"    : code
    }
    return jsonify(response), 200



if __name__ == '__main__':
    timer1 = threading.Timer(1, s_jobs).start()
    app.run(host='0.0.0.0', port=5000)


