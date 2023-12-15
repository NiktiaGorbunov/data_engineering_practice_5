from pymongo import MongoClient
from bson import json_util

import pickle
import json


def whrite_json(items: list, path_name: str):
    json_items = json_util.dumps(items, ensure_ascii=False)
    with open(path_name, 'w', encoding='utf-8') as f:
        f.write(json_items)


def get_from_text(file_name):
    items = []
    with open(file_name, 'r', encoding="utf-8") as f:
        lines = f.readlines()
        item = dict()
        for line in lines:
            if line == '=====\n':
                items.append(item)
                item = dict()
            else:
                line = line.strip()

                splitted = line.split("::")

                if splitted[0] in ('age', 'salary', 'id', 'year'):
                    item[splitted[0]] = int(splitted[1])
                else:
                    item[splitted[0]] = splitted[1]

    return items


def connect():
    client = MongoClient("mongodb://localhost:27017")
    db = client['Practic_4']
    return db.person


def insert_many(collection, data):
    collection.insert_many(data)


def delete_by_salary(collection):
    result = collection.delete_many({
        "$or": [
            {'salary': {"$lt": 25000}},
            {'salary': {"$gt": 175000}},
        ]
    })

    return result.deleted_count


def update_age(collection):
    result = collection.update_many({}, {
        "$inc": {'age': 1}
    })

    return result.modified_count


def mod_salary_by_job(collection):
    result = collection.update_many(
        {
            'job': {"$in": ['Бухгалтер', 'Учитель', 'Инженер', 'Продавец']},
        },
        {
            "$mul": {'salary': 1.05}
        }
    )
    return result.modified_count


def mod_salary_by_city(collection):
    result = collection.update_many(
        {
            'city': {"$nin": ['Кишинев', 'Ереван', 'Краков', 'Сантьяго-де-Компостела']},
        },
        {
            "$mul": {'salary': 1.07}
        }
    )
    return result.modified_count

def mod_salary_complex_query(collection):
    result = collection.update_many(
        {
            'city': 'Кишинев',
            'job': {"$in": ['Бухгалтер', 'Учитель', 'Инженер', 'Продавец']},
            "$or": [
                {"age": {'$gt': 18, '$lt': 25}},
                {"age": {'$gt': 50, '$lt': 65}},
            ]

        },
        {
            "$mul": {'salary': 1.1}
        }
    )
    return result.modified_count


def delete_by_year(collection):
    result = collection.delete_many({
        "$or": [
            {'year': {"$lt": 2015}},
            {'year': {"$gt": 2020}},
        ]
    })

    return result.deleted_count

def main():
    client = connect()

    # data = get_from_text('tasks/task_3_item.text')
    # insert_many(client, data)

    # # задание 1
    # count_del_objects = {'deleted_count': delete_by_salary(client)}
    # whrite_json(count_del_objects, 'answers/task_3_count_del_object.json')
    #
    # # задание 2
    # count_update_objects = {'update_objects': update_age(client)}
    # whrite_json(count_update_objects, 'answers/task_3_count_update_objects.json')

    # # задание 3
    # mod_count = {'mod_count': mod_salary_by_job(client)}
    # whrite_json(mod_count, 'answers/task_3_mod_salary_by_job.json')
    #
    # # задание 4
    # mod_count = {'mod_count': mod_salary_by_city(client)}
    # whrite_json(mod_count, 'answers/task_3_mod_salary_by_city.json')

    # # задание 5
    # mod_count = {'mod_count': mod_salary_complex_query(client)}
    # whrite_json(mod_count, 'answers/task_3_mod_salary_complex_query.json')

    # # задание 6
    # count_del_objects = {'deleted_count': delete_by_year(client)}
    # whrite_json(count_del_objects, 'answers/task_3_delete_by_year.json')



if __name__ == '__main__':
    main()