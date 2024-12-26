import os
import random
import time
import itertools
import csv
import json

from urllib.parse import urljoin
from typing import Any, Union
from concurrent.futures import ThreadPoolExecutor

import redis
import requests

from decouple import config


r = redis.Redis(
    host=config("REDIS_HOST"),
    port=config("REDIS_PORT"),
    password=config("REDIS_PASSWORD"),
    decode_responses=True,
)


BASE_URL = "https://www.bethowen.ru/"
FILENAME = "products.csv"
CONFIG = "config.json"

max_threading = 10
total_parsed_categories = 0
total_parsed_products = 0
http_proxies = list(r.smembers(config("HTTP_PROXIES_KEY")))


def get_data(url: str, method="get", **kwargs) -> Any:
    attempt, max_attempts = 1, 5
    while attempt <= max_attempts:
        try:
            proxy = random.choice(http_proxies)
            proxies = {"http://": f"http://{proxy}"}
            response = requests.request(method, url, proxies=proxies, **kwargs)
            return response.json()
        except Exception as e:
            if attempt >= max_attempts:
                print(f"Failed to get data, ex: {e}")
                attempt += 1
            else:
                print(f"Can't get data, retrying {attempt}")
                time.sleep(2**attempt)
                attempt += 1


def get_categories() -> list[dict]:
    """
    При смене города каталог не обновляется в приложении
    """
    url = urljoin(BASE_URL, "/api/surf/v1/products/categories")
    data = get_data(url)
    return data["categories"]


def get_product_details(product: dict) -> dict:
    url = urljoin(BASE_URL, "/api/surf/v1/products/details")
    params = {"product_id": product["id"]}
    data = get_data(url, params=params)
    return data


def get_offer_details(product_offer: dict) -> dict:
    url = urljoin(BASE_URL, "/api/surf/v1/products/offer/details")
    params = {"offer_id": product_offer["id"]}
    data = get_data(url, params=params)
    return data


def get_products(category: dict, limit: int, offset: int) -> list[dict]:
    url = urljoin(BASE_URL, "/api/surf/v1/products/list")
    params = {
        "category_id": category["id"],
        "limit": limit,
        "offset": offset,
        "sort_type": "popular",
    }
    data = get_data(url, params=params)
    return data


def get_prices(product: dict) -> dict:
    prices = {"price": None, "promo_price": None}
    if product["retail_price"] == product["discount_price"]:
        prices["price"] = product["retail_price"]
    else:
        prices["price"] = product["retail_price"]
        prices["promo_price"] = product["discount_price"]
    return prices


def find_target_shop(target_shop: dict, offer_details: dict) -> Union[dict, None]:
    available_shops = offer_details["availability_info"]["offer_store_amount"]
    target_shop = next(
        (shop for shop in available_shops if int(shop["shop_id"]) == target_shop["id"]),
        None,
    )
    return target_shop


def save_products(products: list[dict]):
    if not products:
        return

    fieldnames = products[0].keys()
    file_exists = os.path.isfile(FILENAME)

    with open(FILENAME, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerows(products)


def parse_product(shop: dict, product: dict) -> list[dict]:
    global total_parsed_products

    offers = list()
    product_detail = get_product_details(product)
    for product_offer in product_detail["offers"]:
        if not product_offer["is_available"]:
            continue

        offer_details = get_offer_details(product_offer)
        if not offer_details["availability_info"]["is_available"]:
            continue

        target_shop = find_target_shop(shop, offer_details)

        offers.append(
            {
                **get_prices(product_offer),
                "code": product_offer["code"],
                "size": product_offer["size"],
                "url": offer_details["sharing_url"],
                "out_of_stock": False if target_shop else True,
                "shop_id": shop["id"],
                "address": shop["address"],
                "product_count": (
                    target_shop["availability"]["text"] if target_shop else None
                ),
            }
        )

    if total_parsed_products % 100 == 0:
        print(f"Total parsed products: {total_parsed_products}.")
    total_parsed_products += 1

    return offers


def parse_products(shop: dict, products: list[dict]) -> list[dict]:
    with ThreadPoolExecutor(max_workers=max_threading) as executor:
        results = executor.map(lambda product: parse_product(shop, product), products)
    return list(itertools.chain(*results))


def parse_category(category: dict, total_categories: int) -> list[dict]:
    global total_parsed_categories

    products = list()
    limit, offset = 100, 0  # Больше 100 сервер уже медленно отвечает
    metadata = {"count": 1000}
    while offset < metadata["count"]:
        products_data = get_products(category, limit, offset)
        products.extend(products_data["products"])
        metadata = products_data["metadata"]
        offset += limit

    total_parsed_categories += 1
    print(
        f"Finish parse category {category["name"]}. "
        f"Collect products: {len(products)}. "
        f"Category {total_parsed_categories}/{total_categories}"
    )
    return products


def collect_categories(
    categories: list[dict], bottom_only=False, result=None
) -> list[dict]:
    if result is None:
        result = list()

    for category in categories:
        if bottom_only:
            if not category.get("subcategories"):
                result.append(category)
        else:
            result.append(category)

        for subcategory in category.get("subcategories", list()):
            collect_categories([subcategory], bottom_only, result)

    return result


def parse_categories(categories: list[dict]) -> list[dict]:
    results = []

    with ThreadPoolExecutor(max_workers=max_threading) as executor:
        futures = []
        for category in categories:
            future = executor.submit(parse_category, category, len(categories))
            futures.append(future)

        for future in futures:
            results.extend(future.result())

    return results


def parse_by_categories(shop: dict, category_ids: list[int]):
    categories = get_categories()
    all_categories = collect_categories(categories)
    allowed_categories = [
        category for category in all_categories if int(category["id"]) in category_ids
    ]
    bottom_categories = collect_categories(allowed_categories, bottom_only=True)
    products = parse_categories(bottom_categories)
    products_for_save = parse_products(shop, products)
    save_products(products_for_save)


def parse_full(shop: dict):
    categories = get_categories()
    bottom_categories = collect_categories(categories, bottom_only=True)
    products = parse_categories(bottom_categories)
    print(f"Total collected products: {len(products)}")
    products_for_save = parse_products(shop, products)
    save_products(products_for_save)


def parse(config_data: dict):
    global max_threading

    shop = config_data["shop"]
    parsing_type = config_data["parsing_type"]
    max_threading = config_data["max_threading"]

    if parsing_type == "full":
        parse_full(shop)
    elif parsing_type == "by_categories":
        categories_ids = config_data["categories_ids"]
        parse_by_categories(shop, categories_ids)


if __name__ == "__main__":
    with open(CONFIG, "r") as config_file:
        config = json.load(config_file)
    parse(config)
