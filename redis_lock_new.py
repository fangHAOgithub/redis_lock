# -*- coding: utf-8 -*-
import redis
import uuid
import time
from threading import Thread
import threading
import random

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
    :param time_out: 锁的超时时间，默认单位为秒
    :return:
    '''
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_time
    lock = "string:redis_lock:" + lock_name
    while time.time() < end:
        if redis_client.set(lock, identifier, ex=time_out, nx=True):
            # 设置锁的同时给锁设置超时时间, 防止进程崩溃导致其他进程无法获取锁
            print(threading.currentThread().ident, "获得了锁", '设置的uuid: ', identifier)
            # 模拟随机崩溃
            r = random.random()
            if r < 0.3:
                # 奔溃后其他线程无法获得锁，只能等待锁超时被释放
                # 一旦崩溃，UUID没有返回，导致无法释放锁
                print('==' * 35)
                print(threading.currentThread().ident, "奔溃了", '崩溃的uuid: ', identifier)
                print('==' * 35)
                break
            return identifier
        elif not redis_client.ttl(lock):
            # 备用方案加过期时间
            r = redis_client.expire(lock, time_out)
            print(r)
        time.sleep(0.001)
    return False


# 释放一个锁
def release_lock(lock_name, identifier):
    '''
    锁释放
    :param lock_name: 锁定名称
    :param identifier: UUID
    :return:
    '''
    lock = "string:redis_lock:" + lock_name
    pip = redis_client.pipeline(True)
    while True:
        try:
            pip.watch(lock)
            lock_value = redis_client.get(lock)
            if not lock_value:
                return True

            if lock_value.decode() == identifier:
                pip.multi()
                pip.delete(lock)
                pip.execute()
                print(threading.currentThread().ident, "释放了锁~~~", '锁的uuid: ', identifier)
                return True
            print(threading.currentThread().ident,
                  '尝试释放锁',
                  lock_value.decode(),
                  f'和传入的UUID是[{identifier}]',
                  "不匹配 / 不是我的设置的锁，无权释放锁，只能干瞪眼（等锁过期后重新请求）")
            pip.unwatch()
            break
        except redis.exceptions.WatchError:
            pass
    return False


def seckill():
    identifier = acquire_lock('resource')
    release_lock('resource', identifier)


if __name__ == '__main__':
    for i in range(10):
        t = Thread(target=seckill)
        t.start()

    '''
    测试结果: 
        16588 获得了锁 设置的uuid:  d6142ad0-0b7a-4c5d-af01-8acc718c20b4
        16588 释放了锁~~~ 锁的uuid:  d6142ad0-0b7a-4c5d-af01-8acc718c20b4
        12412 获得了锁 设置的uuid:  98d59935-eef8-4eba-9473-1e82c60343ce
        12412 释放了锁~~~ 锁的uuid:  98d59935-eef8-4eba-9473-1e82c60343ce
        14832 获得了锁 设置的uuid:  ff408b9e-1795-4274-90b9-95812926d2b7
        14832 释放了锁~~~ 锁的uuid:  ff408b9e-1795-4274-90b9-95812926d2b7
        24760 获得了锁 设置的uuid:  7fe868e3-5ad4-4642-bc63-43b344cffccc
        ======================================================================
        24760 奔溃了 崩溃的uuid:  7fe868e3-5ad4-4642-bc63-43b344cffccc
        ======================================================================
        24760 尝试释放锁 7fe868e3-5ad4-4642-bc63-43b344cffccc 和传入的UUID是[False] 不匹配 / 不是我的设置的锁，无权释放锁，只能干瞪眼（等锁过期后重新请求）
        21660 尝试释放锁 7fe868e3-5ad4-4642-bc63-43b344cffccc 和传入的UUID是[False] 不匹配 / 不是我的设置的锁，无权释放锁，只能干瞪眼（等锁过期后重新请求）
        22624 尝试释放锁 7fe868e3-5ad4-4642-bc63-43b344cffccc 和传入的UUID是[False] 不匹配 / 不是我的设置的锁，无权释放锁，只能干瞪眼（等锁过期后重新请求）
        25120 尝试释放锁 7fe868e3-5ad4-4642-bc63-43b344cffccc 和传入的UUID是[False] 不匹配 / 不是我的设置的锁，无权释放锁，只能干瞪眼（等锁过期后重新请求）
        25256 尝试释放锁 7fe868e3-5ad4-4642-bc63-43b344cffccc 和传入的UUID是[False] 不匹配 / 不是我的设置的锁，无权释放锁，只能干瞪眼（等锁过期后重新请求）
        22664 尝试释放锁 7fe868e3-5ad4-4642-bc63-43b344cffccc 和传入的UUID是[False] 不匹配 / 不是我的设置的锁，无权释放锁，只能干瞪眼（等锁过期后重新请求）
        25228 尝试释放锁 7fe868e3-5ad4-4642-bc63-43b344cffccc 和传入的UUID是[False] 不匹配 / 不是我的设置的锁，无权释放锁，只能干瞪眼（等锁过期后重新请求）
    '''
