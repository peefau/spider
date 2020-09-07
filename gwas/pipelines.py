# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
from twisted.enterprise import adbapi
import pymysql


class GwasPipeline:
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        """
            1、@classmethod声明一个类方法，而对于平常我们见到的叫做实例方法。
            2、类方法的第一个参数cls（class的缩写，指这个类本身），而实例方法的第一个参数是self，表示该类的一个实例
            3、可以通过类来调用，就像C.f()，相当于java中的静态方法
        """
        # 读取settings中配置的数据库参数
        dbparams = dict(
            host=settings['MYSQL_HOST'],
            port=settings['MYSQL_PORT'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',  # 编码要加上，否则可能出现中文乱码问题
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=False
        )
        # **表示将字典扩展为关键字参数,相当于host=xxx,db=yyy...
        dbpool = adbapi.ConnectionPool("pymysql", **dbparams)
        # 相当于dbpool付给了这个类，self中可以得到
        return cls(dbpool)

    def process_item(self, item, spider):
        if item:
            query = self.dbpool.runInteraction(self.do_insert, item)
            #调用异常处理方法
            query.addErrback(self._handle_error, item, spider)
            return item
        else:
            raise DropItem('Missing item')

    def do_delete(self, cursor, item):
        truncate_sql = item.get_truncate_sql()
        cursor.execute(truncate_sql)

    def do_insert(self, cursor, item):
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)


    #错误处理方法
    def _handle_error(self, failure, item, spider):
        return failure