from pymongo import MongoClient
from bson import json_util

import pickle
import json


def whrite_json(items: list, path_name: str):
    json_items = json_util.dumps(items, ensure_ascii=False)
    with open(path_name, 'w', encoding='utf-8') as f:
        f.write(json_items)


def get_from_pkl(file_name):
    jsonArray = []
    with open(file_name, 'rb') as f:
        jsonArray = pickle.load(f)
    return jsonArray


def connect():
    client = MongoClient("mongodb://localhost:27017")
    db = client['Practic_4']
    return db.person


def insert_many(collection, data):
    collection.insert_many(data)


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


def get_freq_by_job(collection):
    q = [
        {
            "$group": {
                "_id": "$job",
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


def max_salary_by_min_age(collection):
    q = [
        {
            '$sort': {
                "age": 1,
                "salary": -1
            }
        },
        {
            '$limit': 1
        }
    ]

    stat = [i for i in collection.aggregate(q)]
    return stat


def min_salary_by_max_age(collection):
    q = [
        {
            '$sort': {
                "age": -1,
                "salary": 1
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
                "salary": {"$gt": 50000},
            }
        },
        {
            "$group": {
                "_id": "$city",
                "max": {"$max": f"$age"},
                "min": {"$min": f"$age"},
                "avg": {"$avg": f"$age"},
            }
        },
        {
            "$sort": {
                "avg": -1
            }
        }
    ]

    stat = [i for i in collection.aggregate(q)]
    return stat


def big_query_2(collection):
    q = [
        {
            "$match": {
                "city": {"$in": ['Кишинев', 'Ереван', 'Краков', 'Сантьяго-де-Компостела']},
                "job": {"$in": ['Бухгалтер', 'Учитель', 'Инженер', 'Продавец']},
                "$or":[
                    {"age": {'$gt': 18, '$lt': 25}},
                    {"age": {'$gt': 50, '$lt': 65}},
                ]

            }
        },
        {
            "$group": {
                "_id": "result",
                "max": {"$max": f"$salary"},
                "min": {"$min": f"$salary"},
                "avg": {"$avg": f"$salary"},
            }
        },

    ]

    stat = [i for i in collection.aggregate(q)]
    return stat


def big_query_3(collection):
    q = [
        {
            "$match": {
                "city": {"$in": ['Кишинев', 'Ереван', 'Краков', 'Сантьяго-де-Компостела']},
                "job": {"$in": ['Учитель']},
            }
        },
        {
            "$group": {
                "_id": "$age",
                "max": {"$max": f"$salary"},
                "min": {"$min": f"$salary"},
                "avg": {"$avg": f"$salary"},
            }
        },
        {
            "$sort": {
                "avg": -1
            }
        }
    ]

    stat = [i for i in collection.aggregate(q)]
    return stat

def main():
    client = connect()

    # data = get_from_pkl('tasks/task_2_item.pkl')
    # insert_many(client, data)

    # задание 1
    stat = get_param_stat_by_column(client, 'result', 'salary')
    # whrite_json(stat, 'answers/task_2_stat_by_salary.json')

    # задание 2
    freq = get_freq_by_job(client)
    # whrite_json(freq, 'answers/task_2_freq_by_job.json')

    # задание 3
    stat = get_param_stat_by_column(client, '$city', 'salary')
    # whrite_json(stat, 'answers/task_2_stat_salary_by_city.json')

    # задание 4
    stat = get_param_stat_by_column(client, '$job', 'salary')
    # whrite_json(stat, 'answers/task_2_stat_salary_by_job.json')

    # задание 5
    stat = get_param_stat_by_column(client, '$city', 'age')
    # whrite_json(stat, 'answers/task_2_stat_age_by_city.json')

    # задание 6
    stat = get_param_stat_by_column(client, '$job', 'age')
    # whrite_json(stat, 'answers/task_2_stat_age_by_job.json')

    # задание 7
    stat = max_salary_by_min_age(client)
    # whrite_json(stat, 'answers/task_2_max_salary_by_min_age.json')

    # задание 8
    stat = min_salary_by_max_age(client)
    # whrite_json(stat, 'answers/task_2_min_salary_by_max_age.json')

    # задание 9
    stat = big_query_1(client)
    # whrite_json(stat, 'answers/task_2_big_query_1.json')

    # задание 10
    stat = big_query_2(client)
    # whrite_json(stat, 'answers/task_2_big_query_2.json')

    # задание 11

    stat = big_query_3(client)
    # whrite_json(stat, 'answers/task_2_big_query_3.json')


if __name__ == '__main__':
    main()
