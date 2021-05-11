# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface


from itemadapter import ItemAdapter
import configparser
import psycopg2
from psycopg2.extras import RealDictCursor, Json

config = configparser.ConfigParser()
config.read('../scrapy.cfg')


class DatabaseWritePipeline:

    def open_spider(self, spider):
        self._db_connection = psycopg2.connect(dbname=config['backend_db']['database_name'],
                                               user=config['backend_db']['username'],
                                               password=config['backend_db']['password'],
                                               host=config['backend_db']['host'],
                                               port=config['backend_db']['port'], cursor_factory=RealDictCursor)
        self.db_cursor = self._db_connection.cursor()


    def close_spider(self, spider):
        self.db_cursor.close()
        self._db_connection.close()

    def process_item(self, item, spider):
        try:

            self.db_cursor.execute("insert into Jobs(vaccancy) values(%s)", (Json(item),))
            self._db_connection.commit()
        except Exception as e:
            print(e)
        return item
