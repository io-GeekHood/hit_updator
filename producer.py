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
import pandas as pd
import io
from minio import Minio
logging.basicConfig(level=logging.DEBUG)
try:
    load_dotenv()
    MinioHost = os.environ.get('MINIO_HOST', 'hitdata.datist.ir:9000')
    S3Host = os.environ.get('MINIO_HOST', 's3://hitdata.datist.ir:9000/')
    MinioUser = os.environ.get('MINIO_USER', 'hitadmin')
    MinioPass = os.environ.get('MINIO_PASSWORD', 'sghllkfij,dhvrndld')
    IMAGE_BUCKET = os.environ.get('IMG_BUCK_NAME', "test")
    REFRENCE_BUCKET = os.environ.get('REF_BUCK_NAME', "test")
    MongoHost = os.getenv('MONGODB_URI')
    SLEEPING = os.getenv('SLEEPING')
    logging.info(f"MinioHost:{MinioHost}")
    logging.info(f"S3Host:{S3Host}")
    logging.info(f"MinioUser:{MinioUser}")
    logging.info(f"MinioPass:{MinioPass}")
    logging.info(f"IMAGE_BUCKET:{IMAGE_BUCKET}")
    logging.info(f"REFRENCE_BUCKET:{REFRENCE_BUCKET}")
    logging.info(f"MongoHost:{MongoHost}")
    logging.info(f"SLEEPING:{SLEEPING}")
except:
    logging.error(f"failed to load ENVS")



def initiator():

    try:
        initiator = Minio(
            MinioHost,
            access_key=MinioUser,
            secret_key=MinioPass,
            secure=True
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
            f"Unable to Access S3 filesystem at: {MinioHost} \n access_key : {MinioUser} \n secret_key : {MinioPass}")


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
    localTz = pytz.timezone('Asia/Tehran')
    striped = dts.strptime(oktime, '%Y/%m/%d %H:%M')
    nowutc = striped.astimezone(localTz)
    gregorian = JalaliDate.to_gregorian(JalaliDate(nowutc)).strftime('%Y-%m-%d %H:%M:%S')
    gregorian = dts.fromisoformat(gregorian).timestamp()
    return round(gregorian)
def transformer(data:dict) -> dict:
    message = {
        "product": {
            "id": str(data["id"]),
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
                {"text": "گوشی بسیار خوبه و همه قس… دستش درد نکنه دیجیکالا", "lang": "IR"},
                {"text": "سری پوکو گوشی‌های هوشمند…هوشمند میان‌رده هستند.", "lang": "IR"}
            ],
            "multi_lingual_brand": [
                {"text": data["brandName"], "lang": "IR"},
                {"text": data["brandLatinName"], "lang": "US"}
            ],
            "media": image_meta_generator(data["name"],data["fullImage"]),
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
    if file == "shop":
        with open("state_storage/shop_state.txt","w") as st:
            st.write(str(number))
    elif file == "data":
        with open("state_storage/data_state.txt","w") as st:
            st.write(str(number))
def get_state(file):
    if file == "shop":
        with open("state_storage/shop_state.txt","r") as st:
            currentShop = st.read()
        return int(currentShop)
    elif file == "data":
        with open("state_storage/data_state.txt","r") as st:
            last_insert = st.read()
        return int(last_insert)

def simple_collect(client:MongoClient,data_object:dict):
    null_model = {
                "url": "https://cdn.okala.com/Media/Index_v2/Product/380636",
                "image_id": "bag_of_shit",
                "file_id": "0-0-0-0"
                }
    db = "okala-fmcg"
    collection = "products"
    if not data_object:
        return null_model
    data = data_object["data"]
    if data and bool(data_object["success"]):
        logging.info("product is available")
        vortex_format = transformer(data)
        try:
            r = client[db][collection].insert_one(vortex_format)
            logging.debug(f"inserted new okala-product with id {r.inserted_id} (success) !")
        except Exception as fail:
            logging.error(f"Failed on mongodb insert phase :{fail}")
            return null_model
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

def get_with_simple_request(url:str,allprx:pd.DataFrame,format:str):
    tries = 0
    while True:
        tries += 1
        if tries > 12:
            break
        random_prx = allprx.sample()
        random_prx = random_prx.to_dict(orient="records")[0]
        try:
            logging.debug(f"using {random_prx}")
            prox = {
                "http": f"http://farid:hg6ykTTBfaGT6nDSzPY9NmKd@{random_prx['host']}:{random_prx['port']}",
                "https": f"http://farid:hg6ykTTBfaGT6nDSzPY9NmKd@{random_prx['host']}:{random_prx['port']}"
            }
            resp = requests.get(url,proxies=prox,timeout=8)
            if resp.status_code == 200:
                logging.info(f"fetched {resp.status_code} with proxy {random_prx} (success)")
                if format == "json":
                    return resp.json()
                elif format == "raw":
                    return resp.raw.read()
        except:
            print(f"failed with  {random_prx} (retrying with new proxy {tries}/15 chances)")
            pass


def minio_image_uploader(filename:str,datas:object,lenght:int):
    tries = 0
    while True:
        tries += 1
        if tries > 11:
            break
        try:
            logging.info(f"tring to insert image in minio bucket {IMAGE_BUCKET}")
            client = Minio(
                MinioHost,
                access_key=MinioUser,
                secret_key=MinioPass,
                secure=True
            )
            client.put_object(bucket_name=IMAGE_BUCKET,object_name=filename,length=lenght,data=datas)
            logging.info(f"image {filename} lenght #{lenght} uploaded with success !")
            break
        except Exception as fail:
            logging.error(f"failed to insert image to minio bucket {fail} (retrying to upload {tries}/10 chances)")

if __name__ == '__main__':




    mongo_client = MongoClient(MongoHost)
    jobs_buffer = []
    refs = pd.read_excel('refs_2.xlsx')
    st = get_state("shop")
    refrence = []
    stores= [5431,5038,5350]
    check = 0
    currentStore = 0
    allprx = get_proxy()
    refrences = []
    checkpoint = 0

    for store in stores[st:]:
        currentStore += 1
        save_state("shop", currentStore)
        count = get_state("data")
        for prod_number in refs["ProductId"][count:]:
            count += 1
            if count > 1000:
                check += 1
                to_save = pd.Series(refrence)
                to_save.to_csv(f"checkpoint_{check}")
                count = 0
            print(f"index #{count}/100")
            target = f"https://api-react.okala.com/C/ReactProduct/GetProductById?productId={prod_number}&storeId={store}"
            result = get_with_simple_request(target,allprx,"json")
            image_info = simple_collect(mongo_client,result)
            save_state("data",prod_number)
            if image_info["file_id"] != "0-0-0-0":
                imageBytes = get_with_simple_request(image_info["url"], allprx,"raw")
                b = bytearray(imageBytes)
                byte_len = len(b)
                value_as_a_stream = io.BytesIO(b)
                minio_image_uploader(image_info["image_id"], value_as_a_stream, byte_len)
                refrences.append(image_info)
                if len(refrences) > 10:
                    checkpoint += 1
                    framed = pd.DataFrame(refrences)
                    framed.to_csv(f"refrences/checkpoint_{checkpoint}")
                    refrences = []