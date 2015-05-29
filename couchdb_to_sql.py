# -*- coding: utf-8 -*-
"""
Created on Thu May 28 10:22:10 2015

@author: gjm

This script get user, item, and rating data stored in CouchDB, and inserts 
it into a MySQL database (which will be created if it does not exist).
"""

#import os
#os.chdir("/Users/gjm/bin/yabr/")
from couchdb import CouchDB
import pymysql.cursors

mysql_url = "localhost"
mysql_db = "yabr"

couchdb_url = "http://127.0.0.1:5984"
couchdb_db = "yabr"

class YabrDBLookupError(LookupError):
    '''Raised when we fail to find an existing game with the specified game_id'''

class YabrDB(object):
    def __init__(self, url, db):
        # charset utf8mb4 is the only proper way to handle true UTF-8 in mySQL.
        # First open connection to server without specifying db
        self.connection = pymysql.connect(host=url,
            user='root',
            passwd='',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        # Create db (if it doesn't already exist)
        self.create_db(db)
        # Now reconnect using the specified db
        self.connection = pymysql.connect(host=url,
            user='root',
            passwd='',
            db=db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        # Create tables (if they don't already exist)
        self.create_tables()
    def create_db(self, db):
        # Can't use parameters with database/table names
        # Technically insecure against the script operator
        sql = ' '.join(['''CREATE DATABASE IF NOT EXISTS ''', db,  
            '''DEFAULT CHARSET=utf8mb4 DEFAULT COLLATE=utf8mb4_unicode_ci'''])
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
        self.connection.commit()        
    def create_tables(self):
        user_data = '''CREATE TABLE IF NOT EXISTS user_data (
            id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user VARCHAR(255) NOT NULL,
            game_id MEDIUMINT UNSIGNED NOT NULL,
            user_rating TINYINT UNSIGNED
            )
            ENGINE=MyISAM 
            '''
        item_data = '''CREATE TABLE IF NOT EXISTS item_data (
            id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, 
            game_id MEDIUMINT UNSIGNED,
            name VARCHAR(255),
            description TEXT,
            minplayers TINYINT UNSIGNED,
            maxplayers SMALLINT UNSIGNED,
            minplaytime SMALLINT UNSIGNED,
            playingtime SMALLINT UNSIGNED,
            maxplaytime SMALLINT UNSIGNED,
            minage TINYINT UNSIGNED,
            year SMALLINT,
            average FLOAT(7,5),
            bayesaverage FLOAT(7,5),
            median FLOAT(7,5),
            stddev FLOAT(7,5),
            usersrated MEDIUMINT UNSIGNED
            )
            ENGINE=MyISAM'''    
        item_categories = '''CREATE TABLE IF NOT EXISTS item_categories (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            category VARCHAR(255),
            item_id INT UNSIGNED NOT NULL,
            FOREIGN KEY (item_id) REFERENCES item_data(id)
            )
            ENGINE=MyISAM 
            DEFAULT CHARSET=utf8mb4 
            COLLATE=utf8mb4_unicode_ci'''    
        item_mechanics = '''CREATE TABLE IF NOT EXISTS item_mechanics (
            id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            mechanic VARCHAR(255),
            item_id MEDIUMINT UNSIGNED NOT NULL,            
            FOREIGN KEY (item_id) REFERENCES item_data(id)
            )
            ENGINE=MyISAM'''    
        item_designers = '''CREATE TABLE IF NOT EXISTS item_designers (
            id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            designer VARCHAR(255),
            item_id MEDIUMINT UNSIGNED NOT NULL,            
            FOREIGN KEY (item_id) REFERENCES item_data(id)
            )
            ENGINE=MyISAM'''    
        item_publishers = '''CREATE TABLE IF NOT EXISTS item_publishers (
            id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            publisher VARCHAR(255),
            item_id MEDIUMINT UNSIGNED NOT NULL,            
            FOREIGN KEY (item_id) REFERENCES item_data(id)
            )
            ENGINE=MyISAM'''    
        item_families = '''CREATE TABLE IF NOT EXISTS item_families (
            id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            family VARCHAR(255),
            item_id MEDIUMINT UNSIGNED NOT NULL,            
            FOREIGN KEY (item_id) REFERENCES item_data(id)
            )
            ENGINE=MyISAM'''    
        with self.connection.cursor() as cursor:
            for sql in (user_data, item_data, item_categories, item_mechanics, 
                        item_designers, item_publishers, item_families):
                cursor.execute(sql)
        self.connection.commit()
    def insert_user_data(self, user, game_id, user_rating):
        sql = '''INSERT INTO user_data (user, game_id, user_rating) VALUES 
            (%s, %s, %s)'''
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (user, game_id, user_rating))
        self.connection.commit()
    def select_item_id(self, game_id):
        sql = '''SELECT id FROM item_data WHERE game_id = %s'''
        with self.connection.cursor() as cursor:
            cursor.execute(sql, game_id)
            result = cursor.fetchone()
            if result:
                item_id = result['id']
            else:
                raise YabrDBLookupError
        return item_id
    def insert_item_data(self, game_id, name, description, minplayers, 
                         maxplayers, minplaytime, playingtime, maxplaytime,
                         minage, year, categories, mechanics, designers, 
                         publishers, families):
        sql = '''INSERT INTO item_data (game_id, name, description, minplayers, 
            maxplayers, minplaytime, playingtime, maxplaytime, minage, year) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (game_id, name, description, minplayers, 
            maxplayers, minplaytime, playingtime, maxplaytime, minage, year))
        self.connection.commit()
        # Not sure if there is a better way to get this information
        item_id = self.select_item_id(game_id)
        # List features are handled separately, and stored in their own tables
        for category in categories:
            self.insert_item_categories(category, item_id)
        for mechanic in mechanics:
            self.insert_item_mechanics(mechanic, item_id)
        for designer in designers:
            self.insert_item_designers(designer, item_id)
        for publisher in publishers:
            self.insert_item_publishers(publisher, item_id)
        for family in families:
            self.insert_item_families(family, item_id)
    def insert_item_categories(self, category, item_id):
        sql = '''INSERT INTO item_categories (category, item_id) VALUES 
            (%s, %s)'''
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (category, item_id))
        self.connection.commit()
    def insert_item_mechanics(self, mechanic, item_id):
        sql = '''INSERT INTO item_mechanics (mechanic, item_id) VALUES 
            (%s, %s)'''        
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (mechanic, item_id))
        self.connection.commit()
    def insert_item_designers(self, designer, item_id):
        sql = '''INSERT INTO item_designers (designer, item_id) VALUES 
            (%s, %s)'''        
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (designer, item_id))
        self.connection.commit()
    def insert_item_publishers(self, publisher, item_id):
        sql = '''INSERT INTO item_publishers (publisher, item_id) VALUES 
            (%s, %s)'''        
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (publisher, item_id))
        self.connection.commit()
    def insert_item_families(self, family, item_id):
        sql = '''INSERT INTO item_families (family, item_id) VALUES 
            (%s, %s)'''        
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (family, item_id))
        self.connection.commit()
    def update_item_ratings(self, game_id, average, bayesaverage, median, 
                            stddev, usersrated):
        # Default value required for try/except/finally
        # However, if there is some other kind of error, the update will fail
        # because item_id cannot be None (NULL)
        item_id = None
        try:
            item_id = self.select_item_id(game_id)
        except YabrDBLookupError:
            # In this case, the item does not yet exist in item_data.
            # So we should create a placeholder for it.
            # Later, the insert_item_data code will have to be modified if 
            # we want to actually use these placeholders.
            # Note that feature list arguments get passed empty lists, rather 
            # than None.
            self.insert_item_data(game_id, None, None, None, None, None, None, 
                None, None, None, [], [], [], [], [])
            # Now there will definitely be an item_id for this game_id
            item_id = self.select_item_id(game_id)
        finally:
            sql = '''UPDATE item_data SET average=%s, bayesaverage=%s, median=%s, 
                stddev=%s, usersrated=%s WHERE id=%s'''
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (average, bayesaverage, median, stddev, 
                                     usersrated, item_id))
            self.connection.commit()     
    def close(self):
        self.connection.close()

def main():    
    mysql = YabrDB(mysql_url, mysql_db)    
    couch = CouchDB(couchdb_url, couchdb_db)

    # Let's call this "user_data" since it tells us what individual users think,
    # vs "item_data" which would tell us the features of particular board games
    print("Getting user data from CouchDB")
    user_data = couch.get_view(couchdb_db, "collections_userratings")

    for user in user_data['rows']:
        # If a user has no collection, they will not be inserted into the database.
        if user['value'].items():
            print(user['key'])
            for game_id, user_rating in user['value'].items():
                # Explicit conversions of string representations of numbers 
                # to numbers. Could also introduce bounds-checking here.
                # In general we assume that game_id exists and is able to be 
                # converted to an int without error. That may not be a safe 
                # assumption.
                game_id = int(game_id)
                mysql.insert_user_data(user['key'], game_id, user_rating)
    
    print("Getting item data from CouchDB")
    item_data = couch.get_view(couchdb_db, "boardgames")
    
    for item in item_data['rows']:
        # If an item has no dictionary data, it will not be inserted into the database.
        if item['value'].items():
            print(item['value']['name'])
            # Explicit conversions of potential string representations of numbers 
            # to numbers. Could also introduce bounds-checking here.
            game_id = int(item['value']['bgg_id'])
            minplayers = None
            maxplayers = None
            minplaytime = None
            playingtime = None
            maxplaytime = None
            minage = None
            year = None
            try:
                minplayers = int(item['value']['minplayers'])
            except ValueError:
                pass
            try:
                maxplayers = int(item['value']['maxplayers'])            
            except ValueError:
                pass
            try:
                minplaytime = int(item['value']['minplaytime'])
            except ValueError:
                pass
            try:                
                playingtime = int(item['value']['playingtime'])
            except ValueError:
                pass
            try:            
                maxplaytime = int(item['value']['maxplaytime'])
            except ValueError:
                pass
            try:
                minage = int(item['value']['minage'])
            except ValueError:
                pass
            try:                
                year = int(item['value']['year'])
            except ValueError:
                pass                
            mysql.insert_item_data(game_id, item['value']['name'], item['value']['description'], 
                minplayers, maxplayers, minplaytime, playingtime, 
                maxplaytime, minage, year, item['value']['categories'], item['value']['mechanics'],
                item['value']['designers'], item['value']['publishers'], item['value']['families'])
    
    # group=True necessary to reduce multiple ratings per game to a single one.
    # These are going to be updating already-existing items with additional info.
    # Furthermore, we could presumably update this info later, whereas the basic 
    # item metadata will not change.
    print("Getting item ratings from CouchDB")
    item_ratings = couch.get_view(couchdb_db, "boardgames_ratings", group=True)
    
    # Note that it is possible to have rating information for an item that we don't have 
    # an item_data for yet. E.g., if a user has in their collection an item 
    # that was added after we scraped the items, or when working with subsets of data.
    # In that case an UPDATE will not work, since we will not find the original item.
    # In that case we should INSERT a new item with NULL metadata, then UPDATE it.
    for item in item_ratings['rows']:
        # If an item has no dictionary data, it will not be inserted into the database.
        if item['value'].items():
            # Explicit conversions of potential string representations of numbers 
            # to numbers. Could also introduce bounds-checking here.
            game_id = int(item['key'])
            print(game_id)
            average = None
            bayesaverage = None
            median = None
            stddev = None
            usersrated = None
            try:
                average = float(item['value']['average'])
            except ValueError:
                pass                
            try:
                bayesaverage = float(item['value']['bayesaverage'])
            except ValueError:
                pass                
            try:
                median = float(item['value']['median'])
            except ValueError:
                pass                
            try:
                stddev = float(item['value']['stddev'])
            except ValueError:
                pass                
            try:
                usersrated = int(item['value']['usersrated'])
            except ValueError:
                pass                
            mysql.update_item_ratings(game_id, average, bayesaverage, 
                median, stddev, usersrated) 

    mysql.close()

if __name__ == "__main__":
    main()
