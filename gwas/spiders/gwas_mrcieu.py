# -*- coding: utf-8 -*-
import scrapy
from gwas.items import GwasItem
from scrapy import cmdline

class GwasMrcieuSpider(scrapy.Spider):
    name = 'gwas_mrcieu'
    allowed_domains = ['gwas.mrcieu.ac.uk']
    offset = 1
    url = "https://gwas.mrcieu.ac.uk/datasets/?page="
    start_urls = [url + str(offset)]

    def parse(self, response):
        items = response.xpath(
            "//div[@class='table-container']/table[@class='table table-striped']/tbody/tr[.]")
        # print(items)
        for it in items:
            # print(it)
            # print(type(it))
            #创建item类
            item = GwasItem()
            # 点. 表示在当前Selector节点下查找数据，而不是从根目录下查找
            item['gwasId'] = it.xpath(".//td[1]/a/text()").extract_first()
            item['year'] = it.xpath(".//td[2]/text()").extract_first()
            item['trait'] = it.xpath(".//td[3]/text()").extract_first()
            item['consortium'] = it.xpath(".//td[4]/text()").extract_first()
            item['sampleSize'] = it.xpath(".//td[5]/text()").extract_first()
            item['numbersOfSNPs'] = it.xpath(".//td[6]/text()").extract_first()
            # print(item)
            yield item

        if self.offset <= 6904:
            self.offset += 1
            yield scrapy.Request(self.url+str(self.offset), callback=self.parse)

if __name__ == '__main__':
    cmdline.execute("scrapy crawl gwas_mrcieu".split())
