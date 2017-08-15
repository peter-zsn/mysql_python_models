#coding=utf-8

"""
@varsion: ??
@author: 张帅男
@file: test.py
@time: 2017/8/15 16:42
"""


import pymysql
from db import Hub

db = Hub(pymysql)

db.add_pool('local',
            host='127.0.0.1', port=3306, user='root', passwd='123456', db='tmp',
            charset='utf8', autocommit=True, pool_size=8, wait_timeout=29
)

row = db.local.user.get(id=6)
print row
print row.id