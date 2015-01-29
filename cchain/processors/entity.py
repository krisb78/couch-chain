import datetime
import logging
import redis
import time

from cchain.processors import base


logger = logging.getLogger(__name__)


class RedisEntityProcessor(base.BaseChangesProcessor):
    """Intended for processing changes coming from multiple sources.
    Assumes that the only information passed with each change is the
    id of the entity that the change affects.

    """

    def __init__(
        self,
        source_set_name,
        target_set_name,
        redis_host='localhost',
        redis_port=6379,
        redis_db=0
    ):
        """

        :param redis_host: the host name of the redis server to use.
        :param redis_port: the port name of the redis server to use.
        :param source_set: the set to add the batches.
        :param target_set: the set to merge the source set with.

        """
        self._source_set_name = source_set_name
        self._target_set_name = target_set_name
        self._redis_server = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            db=redis_db
        )

    def process_changes(self, changes_buffer):

        entity_ids, last_seq = super(
            RedisEntityProcessor,
            self
        ).process_changes(changes_buffer)

        now = datetime.datetime.now()
        timestamp = time.mktime(now.timetuple())

        set_elements = []
        for entity_id in entity_ids:
            set_elements += [
                timestamp,
                entity_id,
            ]

        redis_server = self._redis_server
        source_set_name = self._source_set_name
        redis_server.zadd(source_set_name, *set_elements)
        redis_server.zunionstore(self._target_set_name, [source_set_name])
        redis_server.delete([source_set_name])

        return entity_ids, last_seq
