# -*- coding:utf-8 -*-
from PythonMiddleware.notify import Notify
from PythonMiddleware.graphene import Graphene
from PythonMiddlewarebase.operationids import operations

#import base64
# from PythonMiddlewarebase import memo
#from PythonMiddleware.block import Block
#from PythonMiddleware.account import Account, AccountUpdate
# from PythonMiddlewarebase.account import PrivateKey, PublicKey
# from PythonMiddleware.asset import Asset
# from PythonMiddleware.transactionbuilder import TransactionBuilder

import datetime
from time import sleep
import pymongo
from collections import deque
from threading import Thread, Lock
from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

from config import *
from utils import Logging
from handle_block import logger, parse_operations, handle_operations, init_gauges
import json
from bson import json_util as jsonb

#logger = Logging().getLogger()

block_info_q = deque()  
pending_block_num_q = deque() 
operations_q = deque() 

block_info_deque_lock = Lock()
pending_block_num_deque_lock = Lock()
op_deque_lock = Lock()

def check_block():
    def one_block_check(block_num):
        logger.info('check block number: {}'.format(block_num))
        try:
            block = gph.rpc.get_block(block_num)
            #witness_id = block['witness']
            block_witness = gph.rpc.get_object(gph.rpc.get_object(block['witness'])['witness_account'])['name']
        except Exception as e:
            logger.error('get_object exception. block {}, error {}'.format(block_num, repr(e)))
            return
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

    try:
        gph = Graphene(node=nodeaddress)
        #info = gph.info()
        #logger.info('info: {}'.format(info))

        conn = pymongo.MongoClient(mongodb_params['host'], mongodb_params['port'])
        conn_db = conn[mongodb_params['db_name']]

        sort_limit_result = conn_db.block.find().sort("block_num", -1).limit(1)
        db_last_block_str = jsonb.dumps(sort_limit_result)
        logger.info("db_last_block_str: {}".format(db_last_block_str))
        db_last_block = json.loads(db_last_block_str)

        if len(db_last_block) != 1:
            logger.error("conn_db.block.find().sort('block_num', -1).limit(1) exception")
            conn.close()
            return
        
        start_num = db_last_block[0]['block_num']
        info = gph.info()
        logger.info('info: {}'.format(info))
        last_block_num = info['head_block_number']
        
        increase = int((last_block_num-start_num)/8)+10  # 尽可能处理listen之前新增的区块
        logger.info(">>> start: {}, end: {}, step: {}".format(start_num, last_block_num, increase))
        for index in range(start_num, last_block_num+increase):
            result = conn_db.block.find({'block_num':index})
            if result.count() != 0:
                logger.info('block({}) already exists in mongo db'.format(index))
                continue
            one_block_check(index)
        conn.close()
    except Exception as e:
        logger.error("except: '{}'".format(repr(e)))
    
# 监听区块
def listen_block():
    def on_block_callback(recv_block_id):
        info = gph.info()
        #logger.debug('info: {}'.format(info))
        head_block_id = info['head_block_id']
        block_num = info['head_block_number']
        logger.info('recv_block_id: {}, head_block_id {}, head_block_num {}'.format(recv_block_id, head_block_id, block_num))
        if recv_block_id == head_block_id:
            pending_block_num_deque_lock.acquire()
            pending_block_num_q.append(block_num)
            pending_block_num_deque_lock.release()
            logger.info("pending deque >>>: {}".format(pending_block_num_q))

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

    gph = Graphene(node=nodeaddress)
    from PythonMiddleware.instance import set_shared_graphene_instance
    set_shared_graphene_instance(gph)
    notify = Notify(
        on_block = on_block_callback,
        graphene_instance = gph
    )
    notify.listen()# 启动监听服务


def check_and_listen_block():
    logger.info("------- check block start ------")
    check_block()
    logger.info("------- check block end ------")
    listen_block()

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
                    operations_q.append(operations_list)
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
        if operations_q:
            try:
                op_deque_lock.acquire()
                operations_list = operations_q.popleft()
                op_deque_lock.release()
                handle_operations(operations_list)

                # status = handle_operations(operations_list)
                # if not status:
                #     op_deque_lock.acquire()
                #     block_trx_ops = operations_q.appendleft(operations_list)
                #     op_deque_lock.release()
                #     logger.warn('consume status {}, trx list: {}'.format(status, operations_list))
            except Exception as e:
                logger.error("except: '{}'".format(repr(e)))
        sleep(0.5)

if __name__ == '__main__':
    init_gauges()
    thread_block_analysis = Thread(target=analysis_block)
    thread_block_analysis.start()

    thread_block2db = Thread(target=block2db)
    thread_block2db.start()

    thread_data2db = Thread(target=data2db)
    thread_data2db.start()

    thread_block = Thread(target=check_and_listen_block)
    thread_block.start()

