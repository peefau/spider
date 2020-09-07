# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class GwasItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    gwasId = scrapy.Field()

    year = scrapy.Field()

    trait = scrapy.Field()

    consortium = scrapy.Field()

    sampleSize = scrapy.Field()

    numbersOfSNPs = scrapy.Field()
    #清空表sql
    def get_truncate_sql(self):
        truncate_sql = """
            truncate table s_gwas_mrcieu_ac_uk
        """
        return truncate_sql

    #插入数据sql
    def get_insert_sql(self):
        insert_sql = """
        insert into s_gwas_mrcieu_ac_uk(gwas_id,year,trait,consortium,sample_size,numbers_of_snps)
        VALUES (%s,%s,%s,%s,%s,%s)
        """
        params = (
            self['gwasId'], self['year'], self['trait'], self['consortium'], self['sampleSize'], self['numbersOfSNPs']
        )
        return insert_sql, params
