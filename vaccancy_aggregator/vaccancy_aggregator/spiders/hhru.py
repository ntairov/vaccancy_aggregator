import json
import random
import time

import scrapy
import pandas as pd

class HHruSpider(scrapy.Spider):
    name = "hhru"
    url = 'https://api.hh.ru/vacancies?&enable_snippets=true&only_with_salary=true&showClusters=true&specialization={spec}&area={area}&per_page=100'
    items = []

    area_list = {
        "Москва": {
            "id": 1,
            "jobs": {
                "Программист": {"pages": 0},
                "Сис админ": {"pages": 0},
                "Help Desk": {"pages": 0},
                "Инженер": {"pages": 0},
            }
        },
        "Питер": {
            "id": 2,
            "jobs": {
                "Программист": {"pages": 0},
                "Сис админ": {"pages": 0},
                "Help Desk": {"pages": 0},
            }
        },
        "MO": {
            "id": 2019,
            "jobs": {
                "Программист": {"pages": 0},
                "Сис админ": {"pages": 0},
                "Инженер": {"pages": 0},
            },
            "experience": ["between1And3", 'between3And6', 'moreThan6']
        }
    }

    params_lists = [
        "python", "javascript", "rust", "go", "php", "ruby", "c#", "c++", "perl", "1c", "swift", "android",
        "kotlin", "unity", "django", "ios"
    ]
    prof_list = {"Программист": 1.221, "Сис админ": 1.273, "Help Desk": 1.211, "Инженер": 1.82}

    def start_requests(self):

        try:
            for area in self.area_list:
                for job in self.area_list[area]["jobs"].keys():
                    meta = {'area': area, 'job': job}
                    yield scrapy.Request(url=self.url.format(area=self.area_list[area]["id"], spec=self.prof_list[job]),
                                         callback=self.add_pages, meta=meta)
        except Exception as e:
            print(e)

    def add_pages(self, response):
        data = json.loads(response.text)
        area, job = response.meta.get('area'), response.meta.get('job')

        if data.get("pages") is not None:
            self.area_list[area]["jobs"][job]["pages"] = data["pages"]


        for area in self.area_list:
            for job in self.area_list[area]["jobs"]:
                for page in range(self.area_list[area]["jobs"][job]["pages"]):
                    meta = {'area': area, 'job': job}
                    try:
                        yield scrapy.Request(url=self.url.format(area=self.area_list[area]["id"],
                                                                 spec=self.prof_list[job]) + f"&page={page}",
                                             callback=self.parse_item, meta=meta)
                    except Exception as e:
                        continue

    def parse_item(self, response):
        filter_data = {}


        area, job = response.meta.get('area'), response.meta.get('job')
        try:
            current_data = json.loads(response.body_as_unicode())
        except json.decoder.JSONDecodeError as e:
            print(Exception)
        else:
            for item in current_data["items"]:
                if item["salary"]["from"] is None:
                    item["salary"]["from"] = 0
                if item["salary"]["to"] is None:
                    item["salary"]["to"] = 0
                filter_data.update({
                    item["id"]: {
                        "job_name": job,
                        "name": item["name"].lower(),
                        "area": area,
                        "salary": {
                            "to": item["salary"]["to"],
                            "from": item["salary"]["from"],
                            "currency": item["salary"]["currency"]
                        },
                        "url": item["alternate_url"],
                        "schedule": item["schedule"]["name"].lower(),
                        "published": item["published_at"],
                        "requirement": item["snippet"]["requirement"]
                    }
                })
        print(f'job_total{{job="{job}", city="{area}"}} {current_data["found"]}')

        for fdt in filter_data:
            dt = filter_data[fdt]

            item = {
                "Lang": dt["job_name"],
                "Name": dt["name"],
                "Area": dt["area"],
                "Salary Min": dt["salary"]["from"],
                "Salary Max": dt["salary"]["to"],
                "Currency": dt["salary"]["currency"],
                "URL": dt["url"],
                "Schedule": dt["schedule"],
                "Published": dt["published"],
                "Requirement": dt["requirement"],
            }
            for param in self.params_lists:
                if param in dt["name"]:
                    item["Lang"] = param

            self.items.append(item)
            yield item
        # df = pd.DataFrame(self.items, columns=['Lang', 'Name', 'Area', 'Salary Min', 'Salary Max', 'Currency', 'URL',
        #                                  'Schedule', 'Published', 'Requirement'])

        # yield df.to_csv(r"Hhru.csv", sep=",")