from util import read_json
import json
from db_config import create, batch_insert

path = r'C:\Users\yash.limbasiya\Desktop\Projects\restaurent\Uber_Eat'
TABLE_NAME = "ubereats_restaurants"
BATCH_SIZE = 500

def parse(json_data: str):
    result = {}

    data_path = json_data.get('data', {})
    result['restaurant_name'] = data_path.get('title', {})
    result['restarent_id'] = data_path.get('uuid', {})
    result['slug'] = data_path.get('slug', {})
    result['phone_number'] = data_path.get('phoneNumber', {})
    result['open_or_close'] = data_path.get('isOpen')

    hours = data_path.get("hours", [])
    parsed_hours = []
    for h in hours:
        section_hours = []
        for s in h["sectionHours"]:
            section_hours.append({
                "startTime": s["startTime"] / 60,
                "endTime": s["endTime"] / 60
            })
        parsed_hours.append({
            "dayRange": h["dayRange"],
            "hours": section_hours
        })

    result["hours"] = json.dumps(parsed_hours)
    result['currency'] = data_path.get('currencyCode', {})

    url_list = []
    for url in data_path.get('heroImageUrls', []):
        url_list.append(url.get('url', {}))
    result["image_url"] = json.dumps(url_list)

    locatin_path = data_path.get('location', {})
    result['street_address'] = locatin_path.get('streetAddress', {})
    result['city'] = locatin_path.get('city', {})
    result['country'] = locatin_path.get('country', {})
    result['postalcode'] = locatin_path.get('postalCode', {})
    result['region'] = locatin_path.get('region', {})
    result['location_type'] = locatin_path.get('locationType', {})

    eta_range_path = data_path.get('etaRange', {})
    result['range_of_dilivery'] = eta_range_path.get('text', {})
    result['timing_of_dilivery'] = eta_range_path.get('accessibilityText', {})

    result['category'] = json.dumps(data_path.get('cuisineList', []))

    cate_list = []
    category_path = data_path.get("catalogSectionsMap", {})
    for category in category_path.get('0ad5db85-c10f-5ad6-897c-f8ef6bd5cc78', []):
        cat_path = category.get('payload', {}).get('standardItemsPayload', {}).get('title', {})
        categori = {}
        categori['category'] = cat_path.get('text', {})
        categori['items'] = []

        base_path = category.get('payload', {}).get('standardItemsPayload', {}).get('catalogItems', [])
        for item in base_path:
            items = {}
            items['id'] = item.get('uuid', {})
            items['title'] = item.get('title', {})
            items['item_description'] = item.get('itemDescription', {})
            items['price'] = item.get('priceTagline', {}).get('text', {})
            items['item_image'] = item.get('imageUrl', {}) or None
            categori['items'].append(items)

        cate_list.append(categori)

    result['menu'] = json.dumps(cate_list)

    return result

def main(path : str, table_name : str, batch_size : int):
    create(table_name)
    
    raw = read_json(path)
    batch = []
    for data in raw:
        parsed = parse(data)
        batch.append(parsed)
        if len(batch) >= batch_size:
            batch_insert(table_name, batch)
            batch = []
    if batch:
        batch_insert(table_name, batch)

if __name__ == "__main__":
    main(path, TABLE_NAME, BATCH_SIZE)