import os
import glob
import pandas as pd
import time
import psycopg2
from get_chrome_driver import GetChromeDriver
from selenium import webdriver
import pandas as pd
from pathlib import Path
from supabase import create_client, Client 
from scripts import seleniumDriverWrapper as wrap
from scripts import cardrush
from scripts import marcketCalc
from scripts import supabaseUtil

# カードのデータフレームを取得
def loadCardDf():
    card_list = []
    files = glob.glob("./card/*.csv")
    for file in files:
        readDf = pd.read_csv(
            file,
            encoding="utf_8_sig", sep=",",
            header=0)
        card_list.append(readDf)
    readDfAll = pd.concat(card_list, axis=0, ignore_index=True, sort=True)
    return readDfAll

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
service_key: str = os.environ.get("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(url, key)
supabase.postgrest.auth(service_key)

get_driver = GetChromeDriver()
get_driver.install()
wrapper = wrap.seleniumDriverWrapper()
wrapper.begin(webdriver)
cardrushBot = cardrush.cardrushCsvBot()
loader = marcketCalc.rawLoader()
writer = supabaseUtil.batchWriter()
editor = supabaseUtil.batchEditor()

reader = supabaseUtil.shopCardStockIndexReader()
updated_id_list = reader.read(supabase)

# 全カードを取得する
dfCards = loadCardDf()
id_list = []


# バッチは10件溜まったらPOSTして空にする
batch_items = []
batch_master_id = []
counter = 0

for index, row in dfCards.iterrows():
    if pd.isnull(row['master_id']):
        print('skip:'+row['name'])
        continue

    print('check:'+row['name'])
    
    if row['master_id'] in updated_id_list:
        print('skip:'+row['name'])
        continue

    if row['expansion'] not in ['SV2D','SV2P','SV2a']:
        print('skip:'+row['name'])
        continue

    if row['card_type'] == 'エネルギー':
        print('skip:'+row['name'])
        continue

    if row['is_mirror'] == 'True':
        print('skip:'+row['name'])
        continue

    if row['card_type'] == 'ポケモン' and row['rarity'] in ['C']:
        print('skip:'+row['name'])
        continue

    dataDir = './data/'+row['master_id']

    time.sleep(5)
    cardrushBot.download(wrapper, row['name'], row['cn'], dataDir)

    df = loader.getUniqueRecodes(dataDir)
    records = df.to_dict(orient='records')
    items = editor.getShopStock(row['master_id'],records)

    if len(items) > 0:
        batch_items.extend(items)
        batch_master_id.append(row['master_id'])
        counter += 1

    if len(batch_items) >= 10:
        writer.write(supabase, "shop_card_stock", batch_items)
        batch_items = []
        batch_master_id = []

# 残っていたらPOSTする
if len(batch_items) > 0:
    writer.write(supabase, "shop_card_stock", batch_items)
    batch_items = []
    batch_master_id = []

print('count:'+str(counter))

wrapper.end()
