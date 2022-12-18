import pandas as pd
from pymongo import MongoClient
import os
import logging
from dotenv import load_dotenv
from pprint import pprint
from minio import Minio
logging.basicConfig(level=logging.DEBUG)
try:
    load_dotenv()
    MinioHost = os.environ.get('MINIO_HOST', '178.33.19.30:9000')
    S3Host = os.environ.get('MINIO_HOST', 's3://178.33.19.30/')
    MinioUser = os.environ.get('MINIO_USER', 'hitadmin')
    MinioPass = os.environ.get('MINIO_PASSWORD', 'sghllkfij,dhvrndld')
    IMAGE_BUCKET = os.environ.get('IMG_BUCK_NAME', "okala-images-main")
    REFRENCE_BUCKET = os.environ.get('REF_BUCK_NAME', "okala-refrence-main")
    MongoHost = os.getenv('MONGODB_URI',"mongodb://test:test@172.27.226.107:27011")
    MongoDb = os.getenv('MONGODB_DATABASE',"okala-fmcg")
    MongoCol = os.getenv('MONGODB_COLLECTION',"products")
    logging.info(f"MinioHost:{MinioHost}")
    logging.info(f"S3Host:{S3Host}")
    logging.info(f"MinioUser:{MinioUser}")
    logging.info(f"MinioPass:{MinioPass}")
    logging.info(f"IMAGE_BUCKET:{IMAGE_BUCKET}")
    logging.info(f"REFRENCE_BUCKET:{REFRENCE_BUCKET}")
    logging.info(f"MongoHost:{MongoHost}")
    logging.info(f"DATABASE:{MongoDb}")
    logging.info(f"COLLECTION:{MongoCol}")
except:
    logging.error(f"failed to load ENVS")

REFS = [
        {
            u"$project":
            {
                u"product.id":1,
                u"product.multi_lingual_title.text":1,
                u"product.media":1
            }
        },
        {
            u"$project":
                {
                    "id":"$product.id",
                    "name": "$product.multi_lingual_title.text",
                    "image_id": "$product.media.image_id",
                    "url": "$product.media.url"
                },


        },
        {
        u"$project":{
            "id": "$id",
            "name": "$name",
            "media": {
               "$zip": {
                   "inputs": ["$image_id","$url"]
                }
           }
        }
        },
        {
            u"$unwind": {"path": "$media"}
        },
        {
        u"$project": {
            "id": "$id",
            "name": {"$arrayElemAt": [ "$name", 0 ]},
            "media_id": {"$arrayElemAt": [ "$media", 0 ]},
            "media_url": {"$arrayElemAt": [ "$media", 1 ]},
            }
        },


]

class MongoPipelineIterator:
    def __init__(self, pipe: str = ""):
        try:
            mongoAddress = os.getenv('MONGODB_URI')

            self.pipe = pipe
            logging.debug(f"connecting to db: {mongoAddress} => {MongoDb} {MongoCol}")
            self.client = MongoClient(mongoAddress)
            # result = self.client.admin.command(({'setParameter':1, 'cursorTimeoutMillis': 20000000000}))
            # logging.info(f"no cursor timeout command resulted in {result}")
            self.collection = self.client[MongoDb][MongoCol]
        except Exception as fail:
            logging.error(f"could not connect to database: {mongoAddress} => {MongoDb} {MongoCol} {fail}")

    def __iter__(self):
        try:
            pipeKeywords = {
                "refrence_maker": REFS,
                "": []
            }

            if self.pipe not in list(pipeKeywords.keys()):
                logging.error("could not find any relative pipelines for name", self.pipe)
                return []
            self.cursor = self.collection.aggregate(
                pipeKeywords[self.pipe],
                allowDiskUse=True
            )
            return self
        except Exception as fail:
            logging.error(f"runtime panic on iteration: {pipeKeywords[self.pipe]} {fail}")

    def __next__(self):
        return self.cursor.next()

    def kill(self):
        self.client.close()
    def show_one(self):
        the_one = self.collection.find_one()
        return the_one

def minio_parquet_upload(idx:int,datas:pd.DataFrame):
    local_path = f"refrences/checkpoint_{idx}.parquet"
    try:
        client = Minio(
            MinioHost,
            access_key=MinioUser,
            secret_key=MinioPass,
            secure=False
        )
    except Exception as fail:
        logging.error(f"failed to create connection to minio address {MinioHost} user = {MinioUser} pass = {MinioPass} |\n {fail}")

    try:
        datas["id"] = datas["id"].astype(str)
        datas.index = datas.index.astype(str)
        datas.to_parquet(local_path)
        # client.fput_object(REFRENCE_BUCKET, f"checkpoint_{idx}.parquet",local_path)
        logging.info(f"{local_path} is successfully uploaded archive {REFRENCE_BUCKET} (removing temp file)")
    except Exception as fail:
        logging.error(f"failed to insert parquet file on minio filesystem| {fail}")
    # finally:
    #     try:
    #         logging.error(f"removing {local_path}")
    #         os.remove(local_path)
    #     except:
    #         pass

if __name__ == "__main__":
    load_dotenv()
    collect = MongoPipelineIterator(pipe="refrence_maker")
    container = []
    for doc in collect:
        path = f"/okala-images-main/{doc['media_id']}.jpg"
        doc["image_path"] = path
        container.append(doc)
    refrence = pd.DataFrame(container)
    refrence.set_index("_id",inplace=True,drop=True)
    minio_parquet_upload(0,refrence)