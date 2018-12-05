import requests
import configparser
import pymysql.cursors
import datetime
import os
import time


global new_pastes

class Paste:
    def __init__(self, key_paste, date, scrape_url, full_url, size, expire, title, syntax, username):
        self.key_paste = key_paste
        self.date = int(date)
        self.scrape_url = scrape_url
        self.full_url = full_url
        self.size = int(size)
        self.expire = int(expire)
        self.title = title
        self.syntax = syntax
        self.username = username
        self.hits = 0


def sanitize(paste):
    paste.title = paste.title.replace("'", "''")
    paste.username = paste.username.replace("'", "''")
    paste.syntax = paste.syntax.replace("'", "''")


def set_save_path(key_paste, path):
    conn = pymysql.connect(host='localhost', user='root', password='mysql91.', db='scraping')
    cur = conn.cursor()
    base_query = "UPDATE paste SET file_path='{path}' where key_paste='{key_paste}'"
    query = base_query.format(key_paste=key_paste, path=path)
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def save_document(paste, text):
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    pastes_path = cfg['SCRAPER']['base_folder']
    folder_name = datetime.datetime.now().strftime('%Y-%m-%d')
    folder_path = os.path.join(pastes_path, folder_name)

    try:
        if not os.path.isdir(folder_path):
            os.mkdir(folder_path)
        file_name = "{}.txt".format(paste.key_paste)
        paste_file = os.path.join(folder_path, file_name)
        with open(paste_file, 'wb') as f:
            f.write(text.encode("utf-8"))

        set_save_path(paste.key_paste, paste_file)
    except IOError as e:
        print("ERROR: 001 {}".format(e))
        return False


def exists_paste(key_paste):
    conn = pymysql.connect(host='localhost', user='root', password='mysql91.', db='scraping')
    cur = conn.cursor()
    base_query = "SELECT id_paste from paste where key_paste='{key_paste}'"
    query = base_query.format(key_paste=key_paste)
    #print(query)
    cur.execute(query)
    #exists2 = cur.fetchone()
    #print(exists2)
    #exists = exists2 is not None
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()

    return exists


def store_paste(paste):
    sanitize(paste)
    conn = pymysql.connect(host='localhost', user='root', password='mysql91.', db='scraping')
    cur = conn.cursor()
    base_query = "INSERT INTO paste (key_paste, date, scrape_url, full_url, size, title, expire, syntax, username, hits)" \
                 " values ('{key_paste}',{date},'{scrape_url}','{full_url}',{size},'{title}',{expire}," \
                 "'{syntax}','{username}',{hits})"

    query = base_query.format(key_paste=paste.key_paste, date=paste.date, scrape_url=paste.scrape_url, full_url=paste.full_url,
                              size=paste.size, expire=paste.expire, title=paste.title, syntax=paste.syntax,
                              username=paste.username, hits=paste.hits)
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def get_paste(key_paste):
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    url_paste = cfg["SCRAPER"]['url_paste']

    try:
        r = requests.get(url_paste.format(key_paste))
    except Exception as err:
        print("ERROR 002: {}".format(err))
        return None
    return r.text


def scrap_pastes():
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    url_all = cfg["SCRAPER"]['url_all']
    r = requests.get(url_all)
    json_data = r.json()
    for p in json_data:

        try:

            if not exists_paste(p['key']):
                paste = Paste(p['key'], p['date'], p['scrape_url'], p['full_url'], p['size'], p['expire'], p['title'],
                              p['syntax'], p['user'])
                store_paste(paste)
                text = get_paste(paste.key_paste)
                save_document(paste, text)
                global new_pastes
                new_pastes += 1
        except Exception as err:
            print("ERROR with paste {}: {}".format(p, str(err)))


cont = 0
while True:
    new_pastes = 0
    print("Starting iteration: {}".format(cont))
    scrap_pastes()
    print("Finished iteration. Added {} new pastes. Going to sleep 120 seconds...".format(new_pastes))

    cont += 1
    time.sleep(120)


