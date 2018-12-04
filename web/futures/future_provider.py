#-*- coding: UTF-8 -*-

import core.config.global_var as fd



class FutureProvider(object):

    def __init__(self):
        self.futures = fd.gl_future_dict

    def get_futures(self):
        return self.futures

    def get_future_by_code(self, code):
        return self.futures[code]