import asyncio
from nats.aio.client import Client as NATS
import os
import resource

resource.get_config()


async def listen(loop):
    nc = NATS()
    await nc.connect(io_loop=loop, servers=[os.environ["nat_url"]])

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    await nc.subscribe(os.environ["reply_to"], cb=message_handler)


if __name__ == '__main__':
    pro_home = __file__[:__file__.rfind("/")]
    pid_filename = os.path.join(pro_home, 'pid.log')
    pid_file = open(pid_filename, 'a')
    pid_file.write(str(os.getpid()))
    pid_file.write('\n')
    pid_file.flush()
    pid_file.close()
    loop = asyncio.get_event_loop()
    try:
        asyncio.ensure_future(listen(loop))
        loop.run_forever()
    except Exception as e:
        raise e
    finally:
        loop.close()
