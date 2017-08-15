#coding=utf-8

"""
@varsion: ??
@author: 张帅男
@file: mysql_pool.py
@time: 2017/8/15 10:52
"""

import time
import threading
import logging
import Queue
import types

# MaxBadConnRetries is the number of maximum retries if the driver returns
# (2006, MySQL server has gone away) to signal a broken connection before forcing a new
# connection to be opened.
MaxBadConnRetries = 2

class TimeoutError(Exception):
    pass

class QueuePool:
    def __init__(self, creator, pool_size=5, timeout=2.0, recycle=None):
        """
        :param creator: 回调函数, 返回值为连接对象
        :param pool_size: 连接池大小, 最多保持几个连接, 队列
        :param timeout: 队列阻塞超时时间(秒), 为了防止大量突发连接造成(1040, 'Too many connections')
        :param recycle: 连接保持时间(秒), 不能超过mysql的wait_timeout.， 超过回收
                        查看wait_timeout的方法: show variables like 'wait_timeout'
        """
        self.creator = creator
        self.timeout = timeout
        self.recycle = recycle
        self.q = Queue.Queue(pool_size)
        self.cset = set()                           # 保证队员不重复
        self.overflow = -pool_size                  # 长度
        self._overflow_lock = threading.Lock()

    def inc_overflow(self):
        """连接对象进来"""
        with self._overflow_lock:
            if self.overflow < 0:
                self.overflow += 1
                return True
            else:
                return False

    def dec_overflow(self):
        """连接对象退出"""
        with self._overflow_lock:
            self.overflow -= 1
            return True

    def create_connection(self):
        """创建链接"""
        now = time.time()
        c = self.creator()
        c._pool = self
        c._activetime = now
        c._transacting = False
        return c

    def close(self, conn):
        if not conn.open:
            return
        del conn._pool
        try:
            conn._close()
        except:
            pass
        finally:
            self.dec_overflow()

    def connect(self):
        block = False
        try:
            while 1:
                block = self.overflow >= 0
                """
                调用队列对象的get()方法从队头删除并返回一个项目。可选参数为block，默认为True。如果队列为空且block为True，
                get()就使调用线程暂停，直至有项目可用。如果队列为空且block为False，队列将引发Empty异常。
                """
                c = self.q.get(block, self.timeout)
                if c in self.cset:
                    self.cset.remove(c)
                now = time.time()
                if self.recycle is not None and now - c._activetime >= self.recycle:
                    self.close(c)
                else:
                    return c
        except Queue.Empty:
            if self.overflow >= 0:
                if not block:
                    return self.connect()
                else:
                    raise TimeoutError(
                        "QueuePool limit of size %d, "
                        "connection timed out, timeout %d" %
                        (self.size(), self.timeout))

            if self.inc_overflow():
                try:
                    return self.create_connection()
                except:
                    self.dec_overflow()
                    raise

    def return_conn(self, conn):
        if not conn.open:
            return
        if conn in self.cset:
            return
        if time.time() - conn._activetime >= self.recycle:
            self.close(conn)
            return
        try:
            self.cset.add(conn)
            """
            调用队列对象的put()方法在队尾插入一个项目。put()有两个参数，第一个item为必需的，为插入项目的值；
            第二个block为可选参数，默认为1。
            如果队列当前为空且block为1，put()方法就使调用线程暂停,直到空出一个数据单元。如果block为0，put方法将引发Full异常。
            """
            self.q.put(conn, False)
        except Queue.Full:
            logging.warning('QueuePool Full: %s' % self.q.qsize())
            self.close(conn)

    def size(self):
        return self.q.maxsize

    def len(self):
        return self.q.qsize()

    def clear(self):
        q = self.q
        self.q = Queue.Queue(q.maxsize)
        while 1:
            try:
                c = q.get(False)
                self.close(c)
            except Queue.Empty:
                break

def im_close(conn):
    if hasattr(conn, '_pool'):
        conn._pool.return_conn(conn)

def try_reconnect(conn):
    for i in xrange(MaxBadConnRetries):
        try:
            conn.ping(True)
            return True
        except:
            pass
            # if i < MaxBadConnRetries-1:
            #     time.sleep(1.0)
    return False

def do_query(conn, sql, reconnect=True):
    try:
        conn._activetime = time.time()
        return conn._query(sql)
    except conn._driver.Erroe as e:
        if e[0] == 2006 and reconnect and conn.get_autocommit() and not conn._transacting:
            ok = try_reconnect(conn)
            if ok:
                return do_query(conn, sql, False)
        if e[0] in (2006, 2013):
            conn._pool.close(conn)
        raise

def im_query(conn, sql):
    return do_query(conn, sql, True)

class PoolManager:
    def __init__(self, driver):
        self.driver = driver
        self.pools = {}

    def connect(self, **kw):
        pool_size = kw.pop('pool_size', 8)
        recycle = kw.pop('wait_timeout', 30)

        def creator():
            c = self.driver.connect(**kw)
            c._driver = self.driver
            if hasattr(c, 'query') and not hasattr(c, '_query'):
                c._query = c.query
                c.query = types.MethodType(im_query, c)
            if not hasattr(c, '_close'):
                c._close = c.close
                c.close = types.MethodType(im_close, c)
            return c

        key = (kw['host'], kw['port'], kw['user'], kw['db'])
        pool = self.pools.setdefault(key, QueuePool(creator, pool_size=pool_size, recycle=recycle))
        return pool.connect()