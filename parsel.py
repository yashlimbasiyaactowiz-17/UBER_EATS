from util import read_json_zip_files
import json
from db_config import create, batch_insert
from concurrent.futures import ThreadPoolExecutor
import os

path = r"D:\PDP\PDP"
TABLE_NAME = "ubereats_restaurants"
BATCH_SIZE = 500
NUM_THREADS = 6


def parse(json_data: str):
    result = {}

    data_path = json_data.get('data', {})
    result['restaurant_name'] = data_path.get('title')
    result['restarent_id'] = data_path.get('uuid')
    result['slug'] = data_path.get('slug')
    result['phone_number'] = data_path.get('phoneNumber')
    result['open_or_close'] = data_path.get('isOpen')

    hours = data_path.get("hours", [])
    parsed_hours = []
    for h in (hours or []):
        section_hours = []
        for s in h.get("sectionHours", []):
            section_hours.append({
                "startTime": s.get("startTime", 0) / 60,
                "endTime": s.get("endTime", 0) / 60
            })
        parsed_hours.append({
            "dayRange": h.get("dayRange"),
            "hours": section_hours
        })

    result["hours"] = json.dumps(parsed_hours)
    result['currency'] = data_path.get('currencyCode')

    url_list = []
    for url in (data_path.get('heroImageUrls') or []):
        url_list.append(url.get('url'))
    result["image_url"] = json.dumps(url_list)

    locatin_path = data_path.get('location') or {}
    result['street_address'] = locatin_path.get('streetAddress')
    result['city'] = locatin_path.get('city')
    result['country'] = locatin_path.get('country')
    result['postalcode'] = locatin_path.get('postalCode')
    result['region'] = locatin_path.get('region')
    result['location_type'] = locatin_path.get('locationType')

    eta_range_path = data_path.get('etaRange') or {}
    result['range_of_dilivery'] = eta_range_path.get('text')
    result['timing_of_dilivery'] = eta_range_path.get('accessibilityText')

    result['category'] = json.dumps(data_path.get('cuisineList') or [])

    cate_list = []
    category_path = data_path.get("catalogSectionsMap") or {}
    for category in category_path.get('0ad5db85-c10f-5ad6-897c-f8ef6bd5cc78') or []:
        cat_path = category.get('payload', {}).get('standardItemsPayload', {}).get('title', {})
        categori = {}
        categori['category'] = cat_path.get('text')
        categori['items'] = []

        base_path = category.get('payload', {}).get('standardItemsPayload', {}).get('catalogItems') or []
        for item in base_path:
            items = {}
            items['id'] = item.get('uuid')
            items['title'] = item.get('title')
            items['item_description'] = item.get('itemDescription')
            items['price'] = (item.get('priceTagline') or {}).get('text')
            items['item_image'] = item.get('imageUrl') or None
            categori['items'].append(items)

        cate_list.append(categori)

    result['menu'] = json.dumps(cate_list)

    return result


def process_files(file_list: list, table_name: str, batch_size: int):
    batch = []
    for data in read_json_zip_files(file_list):
        try:
            parsed = parse(data)
            batch.append(parsed)
            if len(batch) >= batch_size:
                batch_insert(table_name, batch)
                batch = []
        except Exception as e:
            print(f"[SKIP] {e}")
    if batch:
        batch_insert(table_name, batch)


def main(path: str, table_name: str, batch_size: int):
    create(table_name)
    all_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.gz')]
    chunk_size = max(1, len(all_files) // NUM_THREADS)
    chunks = [all_files[i:i+chunk_size] for i in range(0, len(all_files), chunk_size)]
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(lambda chunk: process_files(chunk, table_name, batch_size), chunks)


if __name__ == "__main__":
    main(path, TABLE_NAME, BATCH_SIZE)