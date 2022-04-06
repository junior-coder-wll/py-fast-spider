
import umsgpack
from .base import BaseRedisQueue

class LifoRedisQueue(BaseRedisQueue):
    def get_nowait(self):
        ret = self.redis.rpop(self.name)
        if ret is None:
            raise self.Empty
        return umsgpack.unpackb(ret)

    def put_nowait(self, obj):
        if self.lazy_limit and self.last_qsize < self.maxsize:
            pass
        elif self.full():
            raise self.Full
        self.last_qsize = self.redis.rpush(self.name, umsgpack.packb(obj))
        return True