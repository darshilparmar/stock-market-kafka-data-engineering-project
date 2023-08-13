import io
import pandas as pd
import random
import requests

import requests

def get_employee_data():
    url = "write url to fetch employee data"
    session = requests.Session()
    response = session.get(url)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Request failed with status code:", response.status_code)
        return None

def lambda_handler(event, context):
    # You can use API as well to fetch data
    employee_data = get_employee_data()
    print(employee_data)

    name_list = ["Shashank","Amit","Nitin","Manish","Nikhil","Kunal","Vishal"]
    age_list = [29,34,21,23,27,22,20]
    salary_list = [1000,2000,3000,4000,5000,6000,7000]
    random_index = random.randint(0,6)

    return {
        "emp_name" : name_list[random_index],
        "emp_age" : age_list[random_index],
        "emp_salary" : salary_list[random_index]
    }