#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2016 Peng Liu <myme5261314@gmail.com>
#
# Distributed under terms of the gplv3 license.

"""
This file runs the crawler.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import asyncio
import aiohttp
import time

import utils

# url_t = "https://www.dandb.com/search/?search_type=duns&country=&duns=557700898"
url_t = "https://www.dandb.com/search/?search_type=duns&country=&duns="
header = {
    "Host": "www.dandb.com",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, sdch, br",
}

async def get_body(duns, http_session):
    url = url_t + str(duns)
    for _ in range(10):
        try:
            with aiohttp.Timeout(15):
                async with http_session.get(url,headers=header) as res:
                    assert res.status == 200
                    return await res.read()
        except (aiohttp.errors.DisconnectedError, aiohttp.errors.ClientOSError, asyncio.TimeoutError) as e:
            print(e)
    return None

async def producer(resume_duns, work_queue):
    for duns in utils.get_next_duns(resume_duns):
        await work_queue.put(duns)

async def consumer(task_id, work_queue, db_session, http_session):
    while not work_queue.empty():
        duns = await work_queue.get()
        body = await get_body(duns, http_session)
        if not body:
            print("%d processed failed by task %d" % (duns, task_id))
            continue
        print("%d processed by task %d" % (duns, task_id))

def main():
    start = time.time()
    engine = create_engine('mysql+pymysql://root:inmotion@localhost:3306/duns2company')
    DBSession = sessionmaker(bind=engine)
    db_session = DBSession()
    conn = aiohttp.ProxyConnector(proxy="http://localhost:8888")
    http_session = aiohttp.ClientSession(connector=conn)

    resume_duns = utils.get_largest_duns_stored(db_session)
    q = asyncio.Queue(10000)
    tasks = [producer(resume_duns, q)]
    tasks += [consumer(i, q, db_session, http_session) for i in range(100)]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    db_session.close()
    http_session.close()
    print("takes time %f seconds." % time.time() - start)

if __name__ == '__main__':
    main()
