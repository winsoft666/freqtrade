import redis
import threading
import logging
import json

redis_stop_singal = False

logger = logging.getLogger('redis')

def redis_sub_process(bot_id, port):
    r = redis.Redis(host='127.0.0.1', port=port, decode_responses=True)
    sub_obj = r.pubsub()
    sub_obj.subscribe("bot_cmd")
    sub_obj.parse_response()
    while not redis_stop_singal:
        msg = sub_obj.parse_response()

        logger.info(f"Redis recv: {msg[2]}")
        try:
            j = json.loads(msg[2])
            if j.get("bot_id") != bot_id:
                logger.info(f"Skip")
                continue
        except Exception as e:
            logger.exception(f'Fatal exception: {str(e)}')

def start_redis_sub(bot_id, port):
    thread = threading.Thread(target=redis_sub_process, args=(bot_id, port))
    thread.start()
    logger.info(f"Redis listen on {port}")


if __name__ == '__main__':
    start_redis_sub(1, 6379)

