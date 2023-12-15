from pymongo import MongoClient
from bson import json_util

import csv
import json


def whrite_json(items: list, path_name: str):
    json_items = json_util.dumps(items, ensure_ascii=False)
    with open(path_name, 'w', encoding='utf-8') as f:
        f.write(json_items)

def get_from_csv(file_name):
    jsonArray = []
    with open(file_name, "r", encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        for row in rows:
            row['SquareFeet'] = int(row['SquareFeet'])
            row['Bedrooms'] = int(row['Bedrooms'])
            row['Bathrooms'] = int(row['Bathrooms'])
            row['Neighborhood'] = row['Neighborhood']
            row['YearBuilt'] = int(row['YearBuilt'])
            row['Price'] = float(row['Price'])

        jsonArray = rows
    return jsonArray


def connect():
    client = MongoClient("mongodb://localhost:27017")
    db = client['Practic_4']
    return db.houses


def insert_many(collection, data):
    collection.insert_many(data)


def sort_by_price(collection):
    houses = []
    for house in collection.find({}, limit=10).sort({'Price': -1}): # 1 - по возрастанию
        houses.append(house)
    return houses


def filter_by_price(collection):
    houses = []
    for house in collection.find({"YearBuilt": {"$lt": 2000}}, limit=15).sort({'salary': -1}):
        houses.append(house)
    return houses


def complex_filter_by_Bedrooms_and_Bathrooms(collection):
    houses = []
    for house in collection.find({"Bedrooms": {"$lt": 4}, "Bathrooms": {"$lt": 2}}, limit=10).sort({'SquareFeet': -1}):
        houses.append(house)
    return houses


def filter_by_Neighborhood(collection):
    houses = []
    for house in collection.find({"Neighborhood": {"$in": ["Urban", "Rural", "Suburb"]}}, limit=10).sort({'SquareFeet': 1}):
        houses.append(house)
    return houses


def count_obj(collection):
    result = collection.count_documents({
        'Bedrooms': {'$gt': 2, '$lt': 5},
        'Bathrooms': {'$lt': 3},
        'Price': {'$gt': 150000, '$lt': 200000},
    })
    return result


def get_freq_by_Neighborhood(collection):
    q = [
        {
            "$group": {
                "_id": "$Neighborhood",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": -1
            }
        }
    ]

    stat = [i for i in collection.aggregate(q)]
    return stat


def get_param_stat_by_column(collection, column_name, param_stat):
    q = [
        {
            "$group": {
                "_id": f"{column_name}",
                "max": {"$max": f"${param_stat}"},
                "min": {"$min": f"${param_stat}"},
                "avg": {"$avg": f"${param_stat}"},
            }
        }
    ]

    stat = [i for i in collection.aggregate(q)]
    return stat


def min_price_by_max_squareFeet(collection):
    q = [
        {
            '$sort': {
                "SquareFeet": -1,
                "Price": 1,

            }
        },
        {
            '$limit': 1
        }
    ]

    stat = [i for i in collection.aggregate(q)]
    return stat

def big_query_1(collection):
    q = [
        {
            "$match": {
                "Price": {"$lt": 150000},
            }
        },
        {
            "$group": {
                "_id": "$Neighborhood",
                "max": {"$max": f"$SquareFeet"},
                "min": {"$min": f"$SquareFeet"},
                "avg": {"$avg": f"$SquareFeet"},
            }
        },
        {
            "$sort": {
                "avg": 1
            }
        }
    ]

    stat = [i for i in collection.aggregate(q)]
    return stat


def delete_by_price(collection):
    result = collection.delete_many({
        "$or": [
            {'Price}': {"$lt": 250000}},
            {'Price': {"$gt": 50000}},
        ]
    })

    return result.deleted_count


def update_YearBuilt(collection):
    result = collection.update_many({}, {
        "$inc": {'YearBuilt': 1}
    })

    return result.modified_count


def mod_Price_by_Neighborhood(collection):
    result = collection.update_many(
        {
            'Neighborhood': {"$in": ['Rural', 'Suburb']},
        },
        {
            "$mul": {'Price': 1.05}
        }
    )
    return result.modified_count


def mod_Price_by_Urban(collection):
    result = collection.update_many(
        {
            'Neighborhood': 'Urban',
        },
        {
            "$mul": {'Price': 1.1}
        }
    )
    return result.modified_count

def mod_salary_complex_query(collection):
    result = collection.update_many(
        {
            'Neighborhood': 'Urban',
            'Bedrooms': {"$gt": 3},
            "$or": [
                {"YearBuilt": {'$gt': 1970, '$lt': 1990}},
                {"YearBuilt": {'$gt': 2017, '$lt': 2023}},
            ]

        },
        {
            "$mul": {'Price': 1.2}
        }
    )
    return result.modified_count

def main():
    client = connect()

    data = get_from_csv('dataset/housing_price_dataset.csv')
    # insert_many(client, data)

    # # 1.1 Сортировка по стоимости
    # houses = sort_by_price(client)
    # whrite_json(houses, 'answers/task_4/1/sort_by_price.json')
    #
    # # 1.2 Фильтр по стоимости
    # houses = filter_by_price(client)
    # whrite_json(houses, 'answers/task_4/1/filter_by_price.json')
    #
    # # 1.3 Фильтр по комантам
    # houses = complex_filter_by_Bedrooms_and_Bathrooms(client)
    # whrite_json(houses, 'answers/task_4/1/complex_filter_by_Bedrooms_and_Bathrooms.json')
    #
    # # 1.4 Фильтр по районам
    # houses = filter_by_Neighborhood(client)
    # whrite_json(houses, 'answers/task_4/1/sort_filter_by_Neighborhood.json')
    #
    # # 1.5 Подсчет объектов по улсовию
    # houses = count_obj(client)
    # whrite_json(houses, 'answers/task_4/1/count_obj.json')


    # # 2.1 Частотность районов
    # stat = get_freq_by_Neighborhood(client)
    # whrite_json(stat, 'answers/task_4/2/get_freq_by_Neighborhood.json')
    #
    # # 2.2 Статистика цен по районам
    # stat = get_param_stat_by_column(client, '$Neighborhood', 'Price')
    # whrite_json(stat, 'answers/task_4/2/get_param_stat_by_Neighborhood.json')
    #
    # # 2.3 Статистика размера площади домов в зависимости от года постройки
    # stat = get_param_stat_by_column(client, '$YearBuilt', 'SquareFeet')
    # whrite_json(stat, 'answers/task_4/2/get_param_stat_by_YearBuilt.json')
    #
    # # 2.4 Минимальная цена за максимальную площадь
    # stat = min_price_by_max_squareFeet(client)
    # whrite_json(stat, 'answers/task_4/2/min_price_by_max_squareFeet.json')
    #
    # # 2.5 Статистика по площади жилья стоимостью меньше 150000 в различных районах
    # stat = big_query_1(client)
    # whrite_json(stat, 'answers/task_4/2/big_query_1.json')

    # # 3.1 Удалить дома меньше 50000 и больше 250000
    # count_del_objects = delete_by_price(client)
    # whrite_json(count_del_objects, 'answers/task_4/3/count_del_objects.json')
    #
    # # 3.2 Обновить даты постройки
    # mod_count = update_YearBuilt(client)
    # whrite_json(mod_count, 'answers/task_4/3/update_YearBuilt.json')
    #
    # # 3.3  Обновить стоимость у определенных районов
    # mod_count = mod_Price_by_Neighborhood(client)
    # whrite_json(mod_count, 'answers/task_4/3/mod_Price_by_Neighborhood.json')
    #
    # # 3.4 обновить стоимость у построек в городе
    # mod_count = mod_Price_by_Urban(client)
    # whrite_json(mod_count, 'answers/task_4/3/mod_Price_by_Urban.json')
    #
    # # 3.5  Обновить стоимость у построек в городе, где комнат больше 3 и даты постройки в определенных промежутках
    # mod_count = mod_salary_complex_query(client)
    # whrite_json(mod_count, 'answers/task_4/3/mod_salary_complex_query.json')


if __name__ == '__main__':
    main()
