"""Collects data on the occupancy status of each parking lot in Ahuzat Ha-hof website, and saves it to a feather file.
Meant to be executed on Google Cloud Platform.
"""

import requests
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging
from google.cloud import storage
from pyarrow.feather import write_feather
import pyarrow as pa


PARKING_LOT_URL = 'http://www.ahuzot.co.il/Parking/ParkingDetails/?ID={parking_id}'
PARKING_LIST_URL = 'http://www.ahuzot.co.il/Parking/All/'
STATUS_TO_FLOAT = {'panui':0, 'meat':0.7, 'male':1}
ISRAEL_TZ = timezone(timedelta(hours=2))

def getParkingLotName(soup):
  titles = soup.find_all("span", {"class":"Title"})
  if not titles:
    return None
  assert len(titles) == 1, "Found parking with more than one title: %s" % soup
  return titles[0].text


def getParkingLotStatus(lot_name, lot_url):
  soup = BeautifulSoup(requests.get(lot_url).content, features="html.parser")
  status = soup.find_all("td", {"class":"ParkingDetailsTable"})
  if not status:
    return "unknown"
  assert len(status) == 1, "Found parking with more than one status: %s" % lot_url
  imgs = status[0].find_all("img")
  if not imgs:
    return "unknown"
  assert len(imgs) == 1, "Found parking with weird status image: %d, %s" % (len(imgs), lot_url)
  return imgs[0]['src'].split("/")[-1].split(".")[0]


def getAvailableParkingLots():
  soup = BeautifulSoup(requests.get(PARKING_LIST_URL).content, features="html.parser")
  parkings = {}
  for link in soup.find_all("a", {"class":"ParkingLinkX"}):
    name, url = link.text, link['href']
    parkings[name] = url
  return parkings


def getAllLotsStatus(lot_urls):
  pool = ThreadPool(10)
  results = pool.starmap(getParkingLotStatus, lot_urls.items())
  pool.close()
  pool.join()
  lot_results = {}
  for i, name in enumerate(lot_urls):
    lot_results[name] = results[i]
  return lot_results


def loadData(bucket_name='ahuzat-data-bucket', path='data.feather'):
  client = storage.Client()
  bucket = client.get_bucket(bucket_name)
  blob = bucket.get_blob(path)
  if blob:
    reader = pa.BufferReader(blob.download_as_bytes())
    return pd.read_feather(reader)
  return pd.DataFrame()


def saveData(df, bucket_name='ahuzat-data-bucket', path='data.feather'):
  df = df.drop_duplicates(['lot', 'date', 'day', 'hour', 'minute'])
  client = storage.Client()
  bucket = client.get_bucket(bucket_name)
  blob = bucket.blob(path)
  logging.info("Writing dataframe with %d rows after dedup", df.shape[0])
  output_stream = pa.BufferOutputStream()
  write_feather(df, output_stream)
  blob.upload_from_string(output_stream.getvalue().to_pybytes())

def main(data, context):
    lot_urls = getAvailableParkingLots()
    df = loadData()
    logging.info("Read dataframe with %d rows", df.shape[0])
    lot_results = getAllLotsStatus(lot_urls)
    now = datetime.now(tz=ISRAEL_TZ)
    hour = now.hour
    minute = (now.minute // 10) * 10
    day = (now.weekday() + 1) % 7
    date = str(now.date())
    new_rows = []
    for lot in lot_results:
        # Ignore lots with unknown status
        if lot_results[lot] in STATUS_TO_FLOAT:
            new_rows += [{'lot':lot, 'status':STATUS_TO_FLOAT[lot_results[lot]], 
                          'time':now, 'day':day, 'hour':hour, 'minute':minute, 'date':date}]
    logging.info("Found %d new rows", len(new_rows))
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    saveData(df)


if __name__ == "__main__":
    main('data', 'context')