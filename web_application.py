#-*- coding: UTF-8 -*-

import core.config.global_var as fd
import threading
import json
from time import time
from flask import Flask, jsonify, request
from utils.timer_task import update_position_status, update_admin_score


# 实例化APP
app = Flask(__name__)

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
    timer1 = threading.Timer(1, update_position_status).start()
    timer2 = threading.Timer(5, update_admin_score).start()
    app.run(host='0.0.0.0', port=5000)
