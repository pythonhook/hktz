# -*- coding: UTF-8 -*-

import pymysql;
from DBUtils.PooledDB import PooledDB;
import core.config.db_config as Config;


class ConnectionPool(object):
    __pool = None;

    def __enter__(self):
        self.conn = self.getConn();
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor);
        print ("连接池创建conn和cursor");
        return self;

    def getConn(self):
        if self.__pool is None:
            self.__pool = PooledDB(creator=pymysql, mincached=Config.DB_MIN_CACHED , maxcached=Config.DB_MAX_CACHED,
                                   maxshared=Config.DB_MAX_SHARED, maxconnections=Config.DB_MAX_CONNECYIONS,
                                   blocking=Config.DB_BLOCKING, maxusage=Config.DB_MAX_USAGE,
                                   setsession=Config.DB_SET_SESSION,
                                   host=Config.DB_TEST_HOST , port=Config.DB_TEST_PORT ,
                                   user=Config.DB_TEST_USER , passwd=Config.DB_TEST_PASSWORD ,
                                   db=Config.DB_TEST_DBNAME , use_unicode=False, charset=Config.DB_CHARSET);

        return self.__pool.connection()

    """
    @summary: 释放连接池资源
    """
    def __exit__(self, type, value, trace):
        self.cursor.close()
        self.conn.close()
        print("连接池释放conn和cursor")

"""
单例
"""
connection =  ConnectionPool();