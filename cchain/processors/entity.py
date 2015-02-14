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

    def process_change_line(self, change_line):
        """Returns the id of the document affected by the change.

        """

        (change_line, rev, seq, ) = super(
            RedisEntityProcessor,
            self
        ).process_change_line(change_line)

        doc_id = change_line.get('id')

        return (doc_id, rev, seq, )

    def process_changes(self, changes_buffer):

        processed_changes, last_seq = super(
            RedisEntityProcessor,
            self
        ).process_changes(changes_buffer)

        now = datetime.datetime.now()
        timestamp = time.mktime(now.timetuple())

        set_elements = []
        for (entity_id, rev, seq, ) in processed_changes:
            set_elements += [
                timestamp,
                entity_id,
            ]

        redis_server = self._redis_server
        source_set_name = self._source_set_name
        redis_server.zadd(source_set_name, *set_elements)
        redis_server.zunionstore(
            self._target_set_name,
            [source_set_name],
            aggregate='MIN'
        )
        redis_server.delete([source_set_name])

        return processed_changes, last_seq
