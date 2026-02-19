import os
from pymongo import MongoClient, AsyncMongoClient
from ..modules.ServerLogger import ServerLogger

logger = ServerLogger()


class MongoCore:

    instance_details = {}

    mongo_uri = os.environ.get("MONGO_CONNECTION")
    __db_connection = None
    __db = None

    def __init__(self, **kwargs):
        self.instance_details = {**self.instance_details, **kwargs}
        logger.info(f"{logger.doc} connected to {self.instance_details['database']}")
        if kwargs.get("async-client"):
            logger.warn(f"{logger.WIP} Initializing async client")
            self.__db_connection = AsyncMongoClient(
                self.mongo_uri, tls=True, tlsAllowInvalidCertificates=True
            )
        else:
            self.__db_connection = MongoClient(
                self.mongo_uri, tls=True, tlsAllowInvalidCertificates=True
            )
        assert kwargs["database"]
        database = kwargs["database"]
        self.__db = self.__db_connection[database]

    def get_collection(
        self,
        collection_name: str,
    ):
        return self.__db[collection_name]


monet_db = MongoCore(database="diy_monet")
