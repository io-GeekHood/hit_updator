import sys
import time
import requests
from dotenv import load_dotenv
import uuid
import logging
import os.path
import requests
from pymongo import MongoClient
import json
import uuid
from datetime import datetime as dts
import pytz
from persiantools.jdatetime import JalaliDate
from furl import furl
import pandas as pd
import io
from minio import Minio

logging.basicConfig(level=logging.DEBUG)
# docker run --network=host --name okalaorphan --mount type=bind,source="$(pwd)"/state_storage,target=/app/state_storage okala-collector:koskeshi-mode

try:
    load_dotenv()
    MinioHost = os.environ.get('MINIO_HOST', 'minio-nginx-1:9000')
    S3Host = os.environ.get('S3_HOST', 's3://minio-nginx-1/:9000/')
    MinioUser = os.environ.get('MINIO_USER', 'minioadmin')
    MinioPass = os.environ.get('MINIO_PASSWORD', 'sghllkfij,dhvrndld')
    IMAGE_BUCKET = os.environ.get('IMG_BUCK_NAME', "okala-images-one")
    REFRENCE_BUCKET = os.environ.get('REF_BUCK_NAME', "okala-refrence-one")
    MongoHost = os.getenv('MONGODB_URI',"mongodb://test:test@test-mongotest-1:27017")
    MongoDb = os.getenv('MONGODB_DATABASE',"okala-fmcg-one")
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
    sys.exit(1)



def initiator():

    try:
        initiator = Minio(
            MinioHost,
            access_key=MinioUser,
            secret_key=MinioPass,
            secure=False
        )
        img_found = initiator.bucket_exists(IMAGE_BUCKET)

        if not img_found:
            initiator.make_bucket(IMAGE_BUCKET)
        else:
            logging.warning(f"Image-Bucket {IMAGE_BUCKET} already exists")

        ref_found = initiator.bucket_exists(REFRENCE_BUCKET)

        if not ref_found:
            initiator.make_bucket(REFRENCE_BUCKET)
        else:
            logging.warning(f"Refrence-Bucket {REFRENCE_BUCKET} already exists")

    except Exception as fail:
        logging.warning(
            f"Unable to Access S3 filesystem at: {MinioHost} \n access_key : {MinioUser} \n secret_key : {MinioPass}| {fail}")
        pass

def image_meta_generator(name,images):
    buffer = []
    if images is None:
        return []
    for image in images:
        new_id = str(uuid.uuid1())
        url = image
        alt_text = image
        as_item = {
            "product_id":name,
            "image_id": new_id,
            "url": url,
            "alt_text": alt_text
        }
        buffer.append(as_item)

    return buffer

def to_timestamp(oktime:str):
    try:
        localTz = pytz.timezone('Asia/Tehran')
        striped = dts.strptime(oktime, '%Y/%m/%d %H:%M')
        nowutc = striped.astimezone(localTz)
        gregorian = JalaliDate.to_gregorian(JalaliDate(nowutc)).strftime('%Y-%m-%d %H:%M:%S')
        gregorian = dts.fromisoformat(gregorian).timestamp()
        return round(gregorian)
    except:
        logging.info(f"failed to generate timestamp")
        return 1671092305
def transformer(data:dict) -> dict:
    if data["brand"] is None or data["brand"] == "null":
        data["brand"] = {"createdOn":"1396/9/7 20:01"}
    message = {
        "product": {
            "id": data["id"],
            "multi_lingual_title": [
                {"text": data["name"], "lang": "IR"},
                {"text": data["categoryLatinName"], "lang": "US"}
            ],
            "multi_lingual_description": [
                {"text": data["shortDescription"], "lang": "IR"},
                {"text": data["fullDescription"], "lang": "IR"},
                {"text": data["description"], "lang": "IR"}
            ],
            "multi_lingual_review": [
                {"text": data["metaDescription"], "lang": "IR"}
            ],
            "multi_lingual_brand": [
                {"text": data["brandName"], "lang": "IR"},
                {"text": data["brandLatinName"], "lang": "US"}
            ],
            "media": image_meta_generator(data["id"],data["fullImage"]),
            "rating": data["avgRate"],
            "product_url": data["productWebLink"],
            "original_release_date": to_timestamp(data["brand"]["createdOn"]),
            "sellers": [
                {"id": str(data["storeId"]), "name": data["supplierName"], "url": "https://api-react.okala.com"}],
            "price_list": [
                {"currency_code": "IRR", "amount": data["okPrice"]},
            ],
            "discount_percent": float(data["discountPercent"]),
            "is_available": bool(data["exist"]),
            "attributes": [
                {"key": "priority", "value": str(data["priority"]), "rank": 5, "filterable": False},
                {"key": "isDailyOffer", "value": str(data["isDailyOffer"]), "rank": 5, "filterable": False},
            ],
            "category_layers": [data["categoryName"], data["categoryLatinName"], str(data["categoryId"])],
            "tags": [data["caption"]]
        }
    }
    return message


def get_proxy():
    with open("proxies.json","r") as prx:
        asJson = json.load(prx)
        framed = pd.DataFrame.from_dict(asJson)
    return framed


def save_state(file,number):
    if file == "checkpoint":
        with open("state_storage/check_state.txt","w") as st:
            st.write(str(number))
    elif file == "data":
        with open("state_storage/data_state.txt","w") as st:
            st.write(str(number))
def get_state(file):
    if file == "checkpoint":
        with open("state_storage/check_state.txt", "r") as st:
            lastcheck = st.read()
        return int(lastcheck)
    elif file == "data":
        with open("state_storage/data_state.txt","r") as st:
            last_insert = st.read()
        return int(last_insert)

def simple_collect(client:MongoClient,data_object:dict) -> list:
    null_model = [{
                "url": "https://cdn.okala.com/Media/Index_v2/Product/380636",
                "image_id": "bag_of_shit",
                "product_id": "0-0-0-0"
                }]
    db = MongoDb
    collection = MongoCol
    if not data_object:
        return null_model
    data = data_object["data"]
    if data and bool(data_object["success"]):
        logging.info("product is available")
        vortex_format = transformer(data)
        save_state("data", vortex_format["product"]["id"])
        try:
            r = client[db][collection].insert_one(vortex_format)
            logging.debug(f"inserted new okala-product with id {r.inserted_id} (success) !")
        except Exception as fail:
            logging.error(f"Failed on mongodb insert phase :{fail}")
            pass
        try:
            media = vortex_format["product"]["media"]
            logging.warning(f"returning images job with {media}")
            return media
        except Exception as fail:
            logging.error(f"Failed on image download job registeration :{fail}")
            return null_model
    else:
        logging.warning(f"product is not active and has no data !")
        return null_model

def get_data_with_simple_request(url:str,allprx:pd.DataFrame,prid:int):
    stores = [5431, 5038, 5350]
    tries = 0
    store = 0
    url = furl(url)
    while True:
        url.set({"storeId": str(stores[store]),"productId":prid})
        logging.info(f"GET : {url}")
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                if resp.json()["success"]:
                    logging.info("TRING NO PROXY PRIMARY MODE")
                    logging.info(f"fetched {resp.status_code} with no proxy (success)")
                    return resp.json()
                else:
                    logging.info(f"failed on simple request with no proxy on store {str(stores[store])} trying {str(stores[store+1])}")
                    try:
                        url.set({"storeId": str(stores[store+1]), "productId": prid})
                        resp = requests.get(url, timeout=10)
                        if resp.status_code == 200:
                            if resp.json()["success"]:
                                logging.info("TRING NO PROXY PRIMARY MODE")
                                logging.info(f"fetched {resp.status_code} with no proxy (success)")
                                return resp.json()
                            else:
                                logging.info(f"failed on simple request with no proxy on store {str(stores[store+1])} trying {str(stores[store + 2])}")
                                try:
                                    url.set({"storeId": str(stores[store + 2]), "productId": prid})
                                    resp = requests.get(url, timeout=10)
                                    if resp.status_code == 200:
                                        if resp.json()["success"]:
                                            logging.info("TRING NO PROXY PRIMARY MODE")
                                            logging.info(f"fetched {resp.status_code} with no proxy (success)")
                                            return resp.json()
                                        else:
                                            logging.info(f"failed on third store {str(stores[store + 2])}")
                                    else:
                                        continue
                                except:
                                    time.sleep(3)
                                    logging.info(f"failed on simple request with no proxy on store {str(stores[store])}")
                                    continue
                    except:
                        time.sleep(3)
                        logging.info(f"failed on simple request with no proxy on store {str(stores[store])}")
                        continue
        except:
            time.sleep(3)
            logging.info(f"failed on simple request with no proxy on store {str(stores[store])}")
            continue
        tries += 1
        if tries > 12:
            logging.info("PRESS CTRL+C TO KILL !")
            time.sleep(5)
            break
        random_prx = allprx.sample()
        random_prx = random_prx.to_dict(orient="records")[0]
        try:
            logging.debug(f"using {random_prx['host']}")
            prox = {
                "http": f"http://farid:hg6ykTTBfaGT6nDSzPY9NmKd@{random_prx['host']}:{random_prx['port']}",
                "https": f"http://farid:hg6ykTTBfaGT6nDSzPY9NmKd@{random_prx['host']}:{random_prx['port']}"
            }
            resp = requests.get(url,proxies=prox,timeout=10)
            if resp.status_code == 200:
                logging.info(f"fetched {resp.status_code} with proxy {random_prx} (success)")

                return resp.json()

        except:
            logging.info(f"failed with  {random_prx} (retrying with new proxy {tries}/15 chances)")
            pass

def get_image_with_simple_request(url:str):
    tries = 0
    while True:
        logging.info(f"GET : {url}")
        tries += 1
        if tries > 12:
            logging.info("PRESS CTRL+C TO KILL ! (image not found !)")
            time.sleep(5)
            break
        try:
            resp = requests.get(url,timeout=12)
            if resp.status_code == 200:
                logging.info(f"fetched {resp.status_code} image (success)")
                return resp.content
        except:
            logging.info(f"failed to get image from {url} (retrying {tries}/15 chances)")
            pass
def minio_image_uploader(filename:str,datas:object,lenght:int):
    tries = 0
    while True:
        tries += 1
        if tries > 11:
            break
        if lenght == 0:
            break
        try:
            logging.info(f"trying to insert image in minio bucket {IMAGE_BUCKET}")
            client = Minio(
                MinioHost,
                access_key=MinioUser,
                secret_key=MinioPass,
                secure=False
            )
            filename = filename + ".jpg"
            client.put_object(bucket_name=IMAGE_BUCKET,object_name=filename,length=lenght,data=datas)
            logging.info(f"image {filename} lenght #{lenght} uploaded with success !")
            break
        except Exception as fail:
            logging.error(f"failed to insert image to minio bucket {fail} (retrying to upload {tries}/10 chances)")
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
        datas["image_id"] = datas["image_id"].astype(str)
        datas["product_id"] = datas["product_id"].astype(str)
        datas.to_parquet(local_path)
        client.fput_object(REFRENCE_BUCKET, f"checkpoint_{idx}.parquet",local_path)
        logging.info(f"{local_path} is successfully uploaded archive {REFRENCE_BUCKET} (removing temp file)")
    except Exception as fail:
        logging.error(f"failed to insert parquet file on minio filesystem| {fail}")
    finally:
        try:
            logging.error(f"removing {local_path}")
            os.remove(local_path)
        except:
            pass

if __name__ == '__main__':
    initiator()
    mongo_client = MongoClient(MongoHost)
    collection = mongo_client[MongoDb][MongoCol]
    refs = pd.read_excel('refs_2.xlsx')
    refrence = []
    check = 0
    currentStore = 0
    allprx = get_proxy()
    refrences = []
    checkpoint = get_state("checkpoint")
    count = get_state("data")
    for prod_number in refs["ProductId"][count:]:
        found = collection.find_one({"product.id":prod_number})
        if found:
            continue
        count += 1
        target = f"https://api-react.okala.com/C/ReactProduct/GetProductById?"
        result = get_data_with_simple_request(target,allprx,prod_number)
        image_info = simple_collect(mongo_client,result)
        for image in image_info:
            if image["product_id"] != "0-0-0-0":
                imageBytes = get_image_with_simple_request(image["url"])
                b = bytearray(imageBytes)
                byte_len = len(b)
                value_as_a_stream = io.BytesIO(b)
                minio_image_uploader(image["image_id"], value_as_a_stream, byte_len)
                refrences.append(image)
                logging.info(f" #{100-len(refrences)} iteration to save checkpoint !")
            if len(refrences) > 100:
                checkpoint += 1
                framed = pd.DataFrame(refrences)
                logging.info("saving checkpoint as:")
                logging.info(framed.head(5))
                minio_parquet_upload(checkpoint,framed)
                refrences = []
                save_state("checkpoint",checkpoint)
                save_state("data", count)


