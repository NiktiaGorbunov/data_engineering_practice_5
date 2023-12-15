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

        keys = list(rows[0].keys())[0] # названия колонок
        key_list = keys.split(';') # разбиваем на ключи
        for row in rows:
            items = row[keys].split(';')

            item = {
                key_list[0]: items[0],
                key_list[1]: int(items[1]),
                key_list[2]: int(items[2]),
                key_list[3]: items[3],
                key_list[4]: int(items[4]),
                key_list[5]: int(items[5]),
            }

            jsonArray.append(item)
    return jsonArray


def connect():
    client = MongoClient("mongodb://localhost:27017")
    db = client['Practic_4']
    return db.person

def insert_many(collection, data):
    collection.insert_many(data)

def sort_by_salary(collection):
    persons = []
    for person in collection.find({}, limit=10).sort({'salary': -1}): # 1 - по возрастанию
        persons.append(person)
    return persons

def filter_by_age(collection):
    persons = []
    for person in collection.find({"age": {"$lt": 30}}, limit=15).sort({'salary': -1}):
        persons.append(person)
    return persons

def complex_filter_by_city_and_job(collection):
    persons = []
    for person in collection.find({"city": "Кишинев", "job": {"$in": ["Бухгалтер", "IT-специалист", "Архитектор"]}}, limit=10).sort({'age': 1}):
        persons.append(person)
    return persons

def count_obj(collection):
    result = collection.count_documents({
        'age': {'$gt': 25, '$lt': 35},  # 25 < age < 35
        'year': {'$in': [2019, 2020, 2021, 2022]},  # year in [2019 : 2022]
        '$or': [ #  50000 < salary <= 75000 || 125000 < salary < 150000
            {'salary': {'$gt': 50000, '$lt': 75000}},
            {'salary': {'$gt': 125000, '$lt': 155000}},
        ]
    })
    return result

def main():
    client = connect()

    # data = get_from_csv('tasks/task_1_item.csv')
    # insert_many(client, data)

    persons = sort_by_salary(client)
    # whrite_json(persons, 'answers/task_1_sort_by_salary.json')

    persons = filter_by_age(client)
    # whrite_json(persons, 'answers/task_1_filter_by_age.json')

    persons = complex_filter_by_city_and_job(client)
    # whrite_json(persons, 'answers/task_1_filter_by_city_and_job.json')

    objects = {"count": count_obj(client)}
    print(f"Кол-во записей при (25 < age < 35 , year in [2019 : 2022], 50000 < salary < 75000 || 125000 < salary < 150000) - > {objects['count']} ")
    # whrite_json(objects, 'answers/task_1_count_obj.json')


if __name__ == '__main__':
    main()
