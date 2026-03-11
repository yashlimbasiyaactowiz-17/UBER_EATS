import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from util import read_json_zip_files
from db_config import create, batch_insert

path        = r"D:\PDP\PDP"
TABLE_NAME  = "ubereats_restaurants"
BATCH_SIZE  = 500
NUM_THREADS = 6

logging.basicConfig(
    level=logging.INFO,
    filename="app.log",
    encoding="utf-8",
    format="%(asctime)s - %(levelname)s - %(message)s"
)


_parse_error_handler = logging.FileHandler("parsel_error.log", encoding="utf-8")
_parse_error_handler.setLevel(logging.ERROR)
_parse_error_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(_parse_error_handler)



def parse(json_data: dict):
    result = {}

    data_path = json_data.get('data', {})

   
    result['restarent_id'] = data_path.get('uuid')
    result['restaurant_name'] = data_path.get('title')
    result['slug'] = data_path.get('slug')
    result['phone_number']  = data_path.get('phoneNumber')
    result['open_or_close']  = 1 if data_path.get('isOpen') else 0

  
    parsed_hours = []
    for h in (data_path.get('hours') or []):
        section_hours = []
        for s in (h.get('sectionHours') or []):
            section_hours.append({
                "startTime":  round(s.get('startTime', 0) / 60, 2),
                "endTime": round(s.get('endTime',   0) / 60, 2),
                "sectionTitle": s.get('sectionTitle')
            })
        parsed_hours.append({
            "dayRange": h.get('dayRange'),
            "hours": section_hours
        })
    result['hours'] = json.dumps(parsed_hours, ensure_ascii=False)


    result['currency']  = data_path.get('currencyCode')
    url_list = [u.get('url') for u in (data_path.get('heroImageUrls') or []) if u.get('url')]
    result['image_url'] = json.dumps(url_list, ensure_ascii=False)

    loc = data_path.get('location') or {}
    result['street_address'] = loc.get('streetAddress')
    result['city']  = loc.get('city')
    result['country']  = loc.get('country')
    result['postalcode'] = loc.get('postalCode')
    result['region'] = loc.get('region')
    result['location_type']  = loc.get('locationType')

    eta = data_path.get('etaRange') or {}
    result['range_of_dilivery']  = eta.get('text')
    result['timing_of_dilivery'] = eta.get('accessibilityText')

    result['category'] = json.dumps(data_path.get('cuisineList') or [], ensure_ascii=False)

    cate_list   = []
    catalog_map = data_path.get('catalogSectionsMap') or {}

    for key_uuid, sections in catalog_map.items():
        for section in (sections or []):
            sip      = section.get('payload', {}).get('standardItemsPayload', {})
            cat_text = (sip.get('title') or {}).get('text')

            categori = {'category': cat_text, 'items': []}

            for item in (sip.get('catalogItems') or []):
                categori['items'].append({
                    'id': item.get('uuid'),
                    'title': item.get('title'),
                    'item_description': item.get('itemDescription'),
                    'price':  item.get('price'),        # cents e.g. 5200 → $52.00
                    'price_text': (item.get('priceTagline') or {}).get('text'),
                    'item_image':  item.get('imageUrl'),
                    'is_available': item.get('isAvailable'),
                    'is_sold_out':   item.get('isSoldOut'),
                })

            cate_list.append(categori)

    result['menu'] = json.dumps(cate_list, ensure_ascii=False)

    return result


def process_files(file_list: list, table_name: str, batch_size: int):
    batch = []
    total = 0

    for data in read_json_zip_files(file_list):
        try:
            parsed = parse(data)
            batch.append(parsed)

            if len(batch) >= batch_size:
                batch_insert(table_name, batch)
                total += len(batch)
                batch = []

        except Exception as e:
            logging.error(f"[SKIP] parse error: {e}", exc_info=True)

    if batch:
        batch_insert(table_name, batch)
        total += len(batch)

    logging.info(f"Thread done! Total rows inserted: {total}")


def main(path: str, table_name: str, batch_size: int):
    create(table_name)

    all_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.gz')]
    logging.info(f"Total files found: {len(all_files)}")

    chunk_size = max(1, len(all_files) // NUM_THREADS)
    chunks     = [all_files[i:i + chunk_size] for i in range(0, len(all_files), chunk_size)]
    logging.info(f"Splitting into {len(chunks)} chunks of ~{chunk_size} files each")

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(lambda chunk: process_files(chunk, table_name, batch_size), chunks)

    logging.info("script completed")


if __name__ == "__main__":
    logging.info("script started")
    main(path, TABLE_NAME, BATCH_SIZE)