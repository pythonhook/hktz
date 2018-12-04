#-*- coding: UTF-8 -*-

import core.config.global_var as fd
from utils.member_process import MemberProcess

mp = MemberProcess(fd.conn)
mp.process_users();