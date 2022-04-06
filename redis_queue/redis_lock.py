import redis
import pickle
import threading
import socket
import os

socket.gethostname()  # 获取服务器主机名称
os.getpid()  # 获取进程号码
thread_id = threading.current_thread().name

class RedisLock(object):
    def __init__(self, name, host='localhost', port=6379, db=0,
                 maxsize=0, lazy_limit=True, password=None, cluster_nodes=None):
        self.lock_name = name
        self.redis = redis.Redis(host=host, port=port, db=db, password=password)
        self.maxsize = maxsize
        self.lazy_limit = lazy_limit
        self.last_qsize = 0

    def acquire_lock(self, thread_id):
        '''
            thread_id 表名每个线程的唯一标识值，用来判断解锁
        '''
        # 如果存在 ret=0 否则返回0 若给定的 key 已经存在，则 SETNX 不做任何动作
        # SETNX 是 SET if Not eXists(如果不存在，则 SET)的简写
        ret = self.redis.setnx(self.lock_name, pickle.dumps(thread_id))
        if ret == 1:
            # 设置超时
            self.redis.expire(self.lock_name, 3)  # 3秒后解锁  防止死锁
            print("上锁成功")
            return True
        else:
            print("上锁失败")
            return False

    def release_lock(self, thread_id):
        ret = self.redis.get(self.lock_name)
        # 确保解锁还是上锁线程
        if pickle.loads(ret) == thread_id:
            self.redis.delete(self.lock_name)
            print("解锁成功")
            return True
        else:
            print("解锁失败")
            return False


if __name__ == '__main__':
    redis_lock = RedisLock('redis_lock', host='127.0.0.1', db=9)
    import threading
    import socket
    socket.gethostname()  # 获取服务器主机名称
    import os

    os.getpid()  # 获取进程号码
    thread_id = threading.current_thread().name
    print(thread_id)
    if redis_lock.acquire_lock(thread_id):
        print("上锁成功 执行对应的操作")
        redis_lock.release_lock(thread_id)
