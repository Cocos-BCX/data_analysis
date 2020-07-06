# -*- coding:utf-8 -*-
from PythonMiddleware.graphene import Graphene

import sys
import datetime
import time

from config import *
from utils import Logging

logger = Logging().getLogger()

AFTER_DAYS = 7
last_block_date = "1970-01-01" # random default date
result_block_data = {}

def check_block(start_date):
    global last_block_date, AFTER_DAYS, result_block_data
    start_date = start_date

    gph = Graphene(node=nodeaddress)
    info = gph.info()
    logger.info("info: {}".format(info))
    last_block_num = info['head_block_number']
    logger.info("time: {}".format(info["time"]))
    current_time = info["time"]
    current_date = info["time"].split("T")[0]

    start_block_num = 1
    end_block_num = last_block_num

    seconds = compare_time(current_date, start_date)
    logger.info("current_date: {}, start_date: {}, seconds: {}".format(current_date, start_date, seconds))

    if seconds < 3600 * 24 * AFTER_DAYS:
        logger.info("before {} days".format(AFTER_DAYS))
        logger.info("last_block_num: {}, delta: {}".format(last_block_num, 1800 * 24 * AFTER_DAYS))
        end_block_num = last_block_num
        start_block_num = last_block_num - 1800 * 24 * AFTER_DAYS
    else:
        logger.info("after {} days".format(AFTER_DAYS))
        start_block_num = int(last_block_num - seconds/2)
        end_block_num = int(start_block_num + (1800 * 24 * AFTER_DAYS))
        if last_block_num < end_block_num:
            end_block_num = last_block_num
    logger.info('[block num]start: {}, end: {}, last: {}, seconds: {}'.format(start_block_num, end_block_num, last_block_num, seconds))

    for block_num in range(start_block_num, end_block_num+1):
        try:
            block = gph.rpc.get_block(block_num)
            # logger.info("block: {}".format(block))
            timestamp = block["timestamp"]
            block_date = timestamp.split("T")[0]
            
            if block_date != last_block_date:
                # logger.info("last_date: {}, block_num: {}, block: {}".format(last_block_date, block_num, block))
                logger.info("last_date: {}, block_num: {}, block_id: {}, block timestamp: {}".format(last_block_date, 
                    block_num, block["block_id"], block["timestamp"]))
                if last_block_date in result_block_data.keys():
                    logger.info(">>>>>>>>>>>> {}: {}".format(last_block_date, result_block_data[last_block_date]))
                last_block_date = block_date
                result_block_data[block_date] = {
                    "block_total": 0,
                    "trx_total": 0,
                    "ops_total": 0
                }

            block_data = result_block_data[block_date]
            block_data["block_total"] += 1

            transactions = block["transactions"]
            if transactions:
                block_data["trx_total"] += len(transactions)
                for trx in transactions:
                    block_data["ops_total"] += len(trx[1]["operations"])
            result_block_data[block_date] = block_data
        except Exception as e:
            logger.error('get_object exception. block {}, error {}'.format(block_num, repr(e)))
    logger.info("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>> total result: \n{}".format(result_block_data))

def compare_time(time1, time2):
    s_time = time.mktime(time.strptime(time1,'%Y-%m-%d'))
    e_time = time.mktime(time.strptime(time2,'%Y-%m-%d'))
    return int(s_time) - int(e_time)

def compare_time_test():
    result = compare_time('2020-04-17', '2020-04-19')
    logger.info("result: {}".format(result))

if __name__ == '__main__':
    logger.info('args: {}'.format(sys.argv))
    if len(sys.argv) < 2:
        logger.error('Usage: python3 check.py start_date[2020-07-01]')
        sys.exit(1)
    start_date = sys.argv[1]
    check_block(start_date)

'''
AFTER_DAYS = 3 test record:
---------------------------------------------------


'''