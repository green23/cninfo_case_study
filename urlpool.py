#!/usr/bin/env python

"""
URL Pool for crawler to manage URLs
"""
import redis
import time
import urllib.parse as urlparse


class UrlDB:

    def __init__(self):
        self.db = redis.Redis(host='192.168.199.173', port=6379, decode_responses=True)

    def push_to_redis(self, name, url):
        # Redis存储byte型key
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            self.db.sadd(name, url)
            s = True
        except Exception as err:
            s = False
        return s

    def pop_from_redis(self, count):
        urls, index = [], 0
        while self.db.scard("waiting"):
            url = self.db.spop("waiting")
            urls.append(url)
            index += 1
            if index == count:
                break
        print("url: {}".format(len(urls)))
        return urls


class UrlPool:
    '''
    URL Pool for crawler to manage URLs
    '''

    def __init__(self):
        self.db = UrlDB()
        self.failure_threshold = 3  # 失败次数阈值
        self.pending_threshold = 10  # pending的最大时间，过期要重新下载
        self.waiting_count = 0  # waiting 里面的url的个数
        self.proxy_pool = {}  # {url: last_query_time, }  存放hub url
        self.proxy_refresh_span = 0  # 时间间隔

    # 抓取成功失败
    def set_status(self, url, status_code):
        if status_code == 200:
            self.db.push_to_redis("success", url)
            return
        else:
            self.db.push_to_redis("failure", url)
            return

    def add(self, url):
        self.db.push_to_redis("waiting", url)
        self.waiting_count += 1
        return True

    def addmany(self, urls):
        if isinstance(urls, str):
            print('urls is a str !!!!', urls)
            self.add(urls)
        else:
            for url in urls:
                self.add(url)

    def size(self):
        return self.waiting_count

    def empty(self):
        return self.waiting_count == 0
