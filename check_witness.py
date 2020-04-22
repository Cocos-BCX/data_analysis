# -*- coding:utf-8 -*-
from PythonMiddleware.notify import Notify
from PythonMiddleware.graphene import Graphene
from PythonMiddlewarebase.operationids import operations

import pymongo

from config import *
from utils import Logging

logger = Logging(console=True).getLogger()
gph = Graphene(node=nodeaddress)

def witness_check():
    conn = pymongo.MongoClient(mongodb_params['host'], mongodb_params['port'])
    conn_db = conn[mongodb_params['db_name']]

    index = 1
    witness_id_prefix = "1.6."
    while True:
        witness_id = "{}{}".format(witness_id_prefix, index)
        logger.info("witness_id: {}".format(witness_id))
        witness_obj = gph.rpc.get_object(witness_id)
        if witness_obj is None:
            break
        witness_account_obj = gph.rpc.get_object(witness_obj['witness_account'])
        try:
            count = conn_db.block.count({'witness':witness_account_obj["name"]})
            logger.info("witness:{}, generate blocks: {}".format(witness_id, count))
            witness_object = conn_db.witnesses.find_one({'witness_id':witness_id})
            if witness_object is None:
                witness_object = {
                    'witness_id': witness_id,
                    'witness_account_id': witness_account_obj["id"],
                    'witness_account_name': witness_account_obj["name"],
                    "last_block": "",
                    "last_block_time": "",
                }
                logger.info("witness({}) is non-existent".format(witness_id))
            witness_object["total"] = str(count)
            conn_db.witnesses.save(witness_object)
        except Exception as e:
            logger.error("except: '{}'".format(repr(e)))
        index += 1
    conn.close()
    logger.info("witness check done.")

def main():
    witness_check()

if __name__ == '__main__':
    main()
