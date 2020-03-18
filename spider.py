#!/usr/bin/env python3
import traceback
import time
import asyncio
import aiohttp
import urllib.parse as urlparse
import motor.motor_asyncio
from urlpool import UrlPool
import config
import base64
from datetime import datetime, timedelta


class CrawlerAsync:
    def __init__(self):
        self._workers = 0
        self._workers_max = 5
        self.urlpool = UrlPool()
        self.loop = asyncio.get_event_loop()
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.db = motor.motor_asyncio.AsyncIOMotorClient(config.MONGO_URI)["cninfo"]

    async def create_para(self):
        pass

    async def load_hubs(self):
        pass

    async def process(self, url):
        pass

    async def loop_crawl(self):
        await self.create_para()
        await self.load_hubs()
        last_rating_time = time.time()
        counter = 0
        while True:
            tasks = self.urlpool.db.pop_from_redis(self._workers_max)
            if not tasks:
                print('no url to crawl, sleep 10S')
                await asyncio.sleep(10)
                continue
            for url in tasks:
                self._workers += 1
                counter += 1
                print('crawl:', url, self._workers, counter)
                asyncio.ensure_future(self.process(url))

            gap = time.time() - last_rating_time
            if gap > 5:
                rate = counter / gap
                print('\tloop_crawl() rate:%s, counter: %s, workers: %s' % (round(rate, 2), counter, self._workers))
                last_rating_time = time.time()
                counter = 0
            if self._workers >= self._workers_max:
                print('====== got workers_max, sleep 2 sec to next worker =====')
                await asyncio.sleep(2)

    def run(self):
        try:
            self.loop.run_until_complete(self.loop_crawl())
        except KeyboardInterrupt:
            print('stopped by yourself!')
            del self.urlpool
            pass


class StockInfoCrawlerAsync(CrawlerAsync):

    async def load_hubs(self,):
        data = self.db["stock_basic_info"].find({"status": {"$ne": True}})
        urls = []
        async for d in data:
            urls.append(d["SECCODE"])
        self.urlpool.addmany(urls)

    async def process(self, url):
        status, html = await self.fetch(url)
        if status != 200:
            self.urlpool.set_status(url, status)
            return
        html["status"] = True
        await self.db["stock_basic_info"].update_one({"SECCODE": url}, {"$set": html["records"][0]})
        self.urlpool.set_status(url, status)
        self._workers -= 1

    async def fetch(self, scode, headers=None, timeout=40):
        _headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
            "Host": "webapi.cninfo.com.cn",
            "Origin": "http://webapi.cninfo.com.cn",
            "Referer": "http://webapi.cninfo.com.cn/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "mcode": base64.b64encode(str(int(time.time())).encode("utf8")).decode("utf8"),
        }
        url = "http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1018?scode={}".format(scode)
        _headers = headers if headers else _headers
        try:
            async with self.session.post(url, headers=_headers, data={}, timeout=timeout, proxy="http://127.0.0.1:8888",
                                    verify_ssl=False) as response:
                status = response.status
                print(status, scode)
                html = await response.json(content_type=None, encoding='utf-8')
        except Exception as e:
            msg = 'Failed download: {} | exception: {}, {}'.format(url, str(type(e)), str(e))
            print(msg)
            html = ''
            status = 0
        return status, html


class AnnounceCrawlerAsync(CrawlerAsync):

    def __init__(self, start, end):
        super().__init__()
        self.start_str = start
        self.end_str = end

    async def create_para(self):
        start = datetime.strptime(self.start_str, "%Y%m%d")
        end = datetime.strptime(self.end_str, "%Y%m%d")
        while start <= end:
            start += timedelta(days=1)
            _id = "{}&{}".format(start.strftime("%Y%m%d"), 1)
            self.db["announce_para"].update_one({"_id": _id}, {"$set": {"_id": _id, "status": False}}, upsert=True)

    async def load_hubs(self,):
        pages, data = [], self.db["announce_para"].find({"status": {"$ne": True}})
        async for d in data:
            pages.append(d["_id"])
        self.urlpool.addmany(pages)

    async def process(self, url):
        para = {"_id": url.split("&")[0], "page": url.split("&")[1]}
        status, html = await self.fetch(para)
        if status != 200:
            self.urlpool.set_status(url, status)
            return
        if html["announcements"]:
            await self.db["announce"].insert_many(html["announcements"])
        await self.db["announce_para"].update_one({"_id": url}, {"$set": {"status": True}})
        self.urlpool.set_status(url, status)
        if html["hasMore"]:
            self.urlpool.add("{}&{}".format(para["_id"], str(int(para["page"]) + 1)))
        self._workers -= 1

    async def fetch(self, para, headers=None, timeout=40):
        _headers = {
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
            "Host": "www.cninfo.com.cn",
            "Origin": "http://www.cninfo.com.cn",
            "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&keywords=",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        _headers = headers if headers else _headers
        d = "{}-{}-{}".format(para["_id"][:4], para["_id"][4:6], para["_id"][6:])
        data = {
            "pageNum": para["page"],
            "pageSize": "30",
            "column": "szse",
            "tabName": "fulltext",
            "seDate": "{}~{}".format(d, d),
            "isHLtitle": "true",
            "plate": "",
            "stock": "",
            "searchkey": "",
            "secid": "",
            "category": "",
            "trade": "",
        }
        try:
            async with self.session.post(url, data=data, headers=_headers, timeout=timeout, proxy="http://127.0.0.1:8888",
                                    verify_ssl=False) as response:
                status = response.status
                print(status, para)
                html = await response.json(content_type=None, encoding='utf-8')
        except Exception as e:
            msg = 'Failed download: {} | exception: {}, {}'.format(url, str(type(e)), str(e))
            print(msg)
            html = ''
            status = 0
        return status, html


class FundStaticCrawlerAsync(CrawlerAsync):

    def __init__(self, start, end):
        super().__init__()
        self.start_str = start
        self.end_str = end

    async def create_para(self):
        start = datetime.strptime(self.start_str, "%Y%m%d")
        end = datetime.strptime(self.end_str, "%Y%m%d")
        while start <= end:
            start += timedelta(days=1)
            _id = start.strftime("%Y%m%d")
            self.db["fund_static_para"].update_one({"_id": _id}, {"$set": {"_id": _id, "status": False}}, upsert=True)

    async def load_hubs(self,):
        pages, data = [], self.db["fund_static_para"].find({"status": {"$ne": True}})
        async for d in data:
            pages.append(d["_id"])
        self.urlpool.addmany(pages)

    async def process(self, url):
        status, html = await self.fetch(url)
        if status != 200:
            self.urlpool.set_status(url, status)
            return
        if html["records"]:
            await self.db["fund_static"].insert_many(html["records"])
        await self.db["fund_static_para"].update_one({"_id": url}, {"$set": {"status": True}})
        self.urlpool.set_status(url, status)
        self._workers -= 1

    async def fetch(self, para, headers=None, timeout=40):
        _headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
            "Host": "webapi.cninfo.com.cn",
            "Origin": "http://webapi.cninfo.com.cn",
            "Referer": "http://webapi.cninfo.com.cn/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "mcode": base64.b64encode(str(int(time.time())).encode("utf8")).decode("utf8"),
        }
        d = "{}-{}-{}".format(para[:4], para[4:6], para[6:])
        url = "http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1088?tdate={}&sortcode=003002".format(d)
        _headers = headers if headers else _headers
        try:
            async with self.session.post(url, data={}, headers=_headers, timeout=timeout, proxy="http://127.0.0.1:8888",
                                    verify_ssl=False) as response:
                status = response.status
                print(status, para)
                html = await response.json(content_type=None, encoding='utf-8')
        except Exception as e:
            msg = 'Failed download: {} | exception: {}, {}'.format(url, str(type(e)), str(e))
            print(msg)
            html = ''
            status = 0
        return status, html


if __name__ == '__main__':
    nc = StockInfoCrawlerAsync()
    # nc = AnnounceCrawlerAsync("20200101", "20200318")
    # nc = FundStaticCrawlerAsync("20200101", "20200318")
    nc.run()
