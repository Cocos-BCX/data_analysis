# -*- coding:utf-8 -*-
from PythonMiddleware.notify import Notify
from PythonMiddleware.graphene import Graphene
from PythonMiddlewarebase.operationids import operations


import sys
import pymongo
import datetime
from time import sleep
from collections import deque
from threading import Thread, Lock
from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

from config import *
from utils import Logging
from handle_block import logger, parse_operations, handle_operations, init_gauges

#logger = Logging().getLogger()

block_info_q = deque()  
pending_block_num_q = deque() 
op_d = deque() 

#thread lock
block_info_deque_lock = Lock()
pending_block_num_deque_lock = Lock()
op_deque_lock = Lock()

def check_block(args):
    def one_block_check(block_num):
        logger.info('recv block number: {}'.format(block_num))
        try:
            block = gph.rpc.get_block(block_num)
            #witness_id = block['witness']
            block_witness = gph.rpc.get_object(gph.rpc.get_object(block['witness'])['witness_account'])['name']
        except Exception as e:
            logger.error('get_object exception. block {}, error {}'.format(block_num, repr(e)))
        block_time = block['timestamp']
        transactions = block["transactions"]
        witness_sign = block['witness_signature']
        trx_total = 0
        ops_total = 0
        transactions_id = []
        if transactions:
            trx_total = len(transactions)
            for trx in transactions:
                transactions_id.append(trx[0])
                ops_total += len(trx[1]["operations"])
        block_data = {
            "block_num": block_num,
            "time": block_time,
            "witness": block_witness,
            "witness_sign": witness_sign,
            "transactions_total": trx_total,
            "transactions_id": transactions_id,
            "operations_total": ops_total
        }
        block_info_deque_lock.acquire()
        block_info_q.append(block_data)
        block_info_deque_lock.release()

    start = args[0]
    end = args[1]
    gph = Graphene(node=nodeaddress)
    info = gph.info()
    last_block_num = info['head_block_number']
    #logger.info('last_block_num: {}, block start: {}, end: {}, info: {}'.format(last_block_num, start, end, info))
    logger.info('last_block_num: {}, block start: {}, end: {}'.format(last_block_num, start, end))
    if start > last_block_num:
        logger.error("start:{} < end:{}".format(start, end))
        return
    if end > last_block_num:
        end = last_block_num
    conn = pymongo.MongoClient(mongodb_params['host'], mongodb_params['port'])
    conn_db = conn[mongodb_params['db_name']]
    for index in range(start, end+1):
        result = conn_db.block.find({'block_num':index})
        if result.count() == 0:
            logger.info('check block number: {}'.format(index))
            one_block_check(index)
        else:
            logger.info('block({}) already exists in mongodb'.format(index))
        sleep(0.1)
    conn.close()

# 解析区块
def analysis_block():
    gph = Graphene(node=nodeaddress)
    from PythonMiddleware.instance import set_shared_graphene_instance
    set_shared_graphene_instance(gph)
    while 1:
        if pending_block_num_q:
            try:
                pending_block_num_deque_lock.acquire()
                block_num = pending_block_num_q.popleft()
                pending_block_num_deque_lock.release()
                logger.debug('pop block number: {}'.format(block_num))
                try:
                    block_info = gph.rpc.get_block(block_num)
                    time = block_info["timestamp"]
                    transactions = block_info["transactions"]
                    operations_list = parse_operations(gph, block_num, time, transactions)
                    #logger.debug('block: {}, trx_list: {}'.format(block_num, operations_list))
                except Exception as e:
                    logger.error('parse block exception. block {}, error {}'.format(block_num, repr(e)))
                if operations_list:
                    op_deque_lock.acquire()
                    op_d.append(operations_list)
                    op_deque_lock.release()
            except Exception as e:
                logger.error("pending_block_num_q: {}, except: '{}'".format(pending_block_num_q, repr(e)))
        sleep(0.7)

#将区块数据写入数据库中block表中
def block2db():
    while 1:
        if block_info_q:
            try:
                #global block_info_deque_lock
                block_info_deque_lock.acquire()
                block = block_info_q.popleft()
                block_info_deque_lock.release()
                #update mongodb
                conn = pymongo.MongoClient(mongodb_params['host'], mongodb_params['port'])
                conn_db = conn[mongodb_params['db_name']]
                try: 
                    conn_db.block.insert_one({
                        'block_num': block["block_num"], 
                        'time': block["time"], 
                        'witness': block["witness"], 
                        'witness_sign': block["witness_sign"], 
                        'transactions_id': str(block["transactions_id"]), 
                        'transactions_total': block["transactions_total"], 
                        'operations_total': block["operations_total"]
                    })
                except Exception as e:
                    logger.error("block: {}, except: '{}'".format(block["block_num"], repr(e)))
                finally:
                    conn.close()
                logger.info('block num: {} done.'.format(block["block_num"]))
            except Exception as e:
                logger.error("except: '{}'".format(repr(e)))
        sleep(0.7)

#将区块解析过的数据写入到数据库中的op表和transaction表中
def data2db(): 
    while 1:
        if op_d:
            try:
                op_deque_lock.acquire()
                operations_list = op_d.popleft()
                op_deque_lock.release()
                handle_operations(operations_list)

                # status = handle_operations(operations_list)
                # if not status:
                #     op_deque_lock.acquire()
                #     block_trx_ops = op_d.appendleft(operations_list)
                #     op_deque_lock.release()
                #     logger.warn('consume status {}, trx list: {}'.format(status, operations_list))
            except Exception as e:
                logger.error("except: '{}'".format(repr(e)))
        sleep(0.5)


if __name__ == '__main__':
    logger.info('args: {}'.format(sys.argv))
    if len(sys.argv) < 3:
        logger.error('Usage: python3 check.py block_number_start, block_number_end')
        sys.exit(1)
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    if start > end or start <= 0 or end <= 0:
        logger.error('block_number_start: {} > block_number_end: {} or start <= 0 or end <= 0'.format(start, end))
        sys.exit(1)
    args = [start, end]
    init_gauges()
    t1 = Thread(target=check_block, args=(args,))
    t1.start()
    t2 = Thread(target=block2db)
    t2.start()
    t3 = Thread(target=analysis_block)
    t3.start()
    t4 = Thread(target=data2db)
    t4.start()
