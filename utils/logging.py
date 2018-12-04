import logging
import logging.handlers

class Log:
    # 日志文件名称
    __file = 'hktz.log'
    __handler = False
    # 输出格式
    __fmt = '%(asctime)s - %(filename)s:[line:%(lineno)s] - %(name)s - %(message)s'

    def __init__(self):
        logging.basicConfig(filename=self.__file, filemode='a+', format=self.__fmt)
        #打印
        self.__handler = logging.StreamHandler()
        self.__handler.setLevel(logging.INFO)

        #设置格式
        formatter = logging.Formatter(self.__fmt)
        self.__handler.setFormatter(formatter)
        return

    #获取实例
    def getInstance(self, strname):
        logger = logging.getLogger(strname)
        logger.addHandler(self.__handler)
        logger.setLevel(logging.DEBUG)
        return logger