# -*- coding: utf-8 -*-
import redis
import uuid
import time
from threading import Thread
import threading

# 连接redis
redis_client = redis.Redis(host="localhost",
                           port=6379,
                           # password=password,
                           db=10)


def acquire_lock(lock_name, acquire_time=10, time_out=10):
    '''
    #   获取一个分布式锁
    :param lock_name: 锁定名称
    :param acquire_time: 客户端等待获取锁的时间
    :param time_out: 锁的超时时间
    :return:
    '''
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_time
    lock = "string:redis_lock:" + lock_name
    while time.time() < end:
        if redis_client.setnx(lock, identifier):
            # 给锁设置超时时间, 防止进程崩溃导致其他进程无法获取锁
            redis_client.expire(lock, time_out)
            print('UUID', identifier)
            return identifier
        elif not redis_client.ttl(lock):
            redis_client.expire(lock, time_out)
        time.sleep(0.001)
    return False


# 释放一个锁
def release_lock(lock_name, identifier):
    '''
    :param lock_name: 锁定名称
    :param identifier: UUID
    :return:
    '''
    """通用的锁释放函数"""
    lock = "string:redis_lock:" + lock_name
    pip = redis_client.pipeline(True)
    while True:
        try:
            pip.watch(lock)
            lock_value = redis_client.get(lock)
            print(lock_value)
            if not lock_value:
                return True

            if lock_value.decode() == identifier:
                pip.multi()
                pip.delete(lock)
                pip.execute()
                return True
            pip.unwatch()
            break
        except redis.exceptions.WatchError:
            pass
    return False


def seckill():
    identifier = acquire_lock('resource')
    print(threading.currentThread().ident, "获得了锁")
    release_lock('resource', identifier)


for i in range(10):
    t = Thread(target=seckill)
    t.start()
