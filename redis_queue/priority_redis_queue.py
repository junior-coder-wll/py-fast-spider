import umsgpack
from .base import BaseRedisQueue

"""实现优先级权重队列"""


class PriorityRedisQueue(BaseRedisQueue):
    """ 利用redis有序集合实现存取 """

    def qsize(self):
        '''获取redis有序集合的长度'''
        self.last_qsize = self.redis.zcard(self.name)
        return self.last_qsize

    def put_nowait(self, obj):
        """
        obj是元祖 由权重和值组成
        """
        if self.lazy_limit and self.last_qsize < self.maxsize:
            pass
        elif self.full():
            raise self.Full
        mapping = {umsgpack.packb(obj[1]): float(obj[0])}
        self.last_qsize = self.redis.zadd(self.name, dict(mapping))
        return True

    def get_nowait(self):
        """
        -1,-1 取权重值最大的
        0,0 取权重值最小的
        """
        ret = self.redis.zrange(self.name, -1, -1)
        self.redis.zrem(self.name, ret[0])
        if not ret:
            raise self.Empty
        return umsgpack.unpackb(ret[0])
