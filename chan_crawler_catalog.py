import requests
from dotenv import load_dotenv
import os
from datetime import datetime
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from time import time
from faktory import Worker, connection
import logging 
from chan_crawler_functions import *

load_dotenv()
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
FAKTORY_URL = os.environ.get('FAKTORY_URL')
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')

CHAN_BASE_URL = """https://a.4cdn.org"""
DEFAULT_UNIX_TIME = 1696118400 # 2023-10-01 12:00:00 AM UTC

crawled_threads = {}
board_last_modified = {}

for board in os.environ.get('BOARDS').split(','):
	crawled_threads[board] = set()
	board_last_modified[board] = ""

def crawlCatalog(board: str):
	headers = {"If-Modified-Since": board_last_modified[board], "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0"}
	
	# thread_id = 0 for the OP of a thread
	timestamp_sql = """SELECT MAX(timestamp) 
						FROM chan_posts 
						WHERE 
							board LIKE %s AND 
							thread_id = 0""" 

	# get (formerly) living/unarchived threads
	get_alive_threads_sql = """SELECT post_id 
								FROM chan_posts 
								WHERE 
									board LIKE %s AND 
									thread_id = 0 AND 
									is_dead LIKE '0'""" 
	new_threads_to_crawl = set()
	if headers["If-Modified-Since"] == "":
		conn = connection_pool.getconn()
		with conn.cursor() as cur:
			# get the maximum timestamp thread in our db (want to get threads newer than that if we restarted program)
			cur.execute(timestamp_sql, (board,))
			timestamp = cur.fetchone()
			if timestamp is not None and timestamp[0] is not None:
				dt = datetime.fromtimestamp(timestamp[0])
			else:
				dt = datetime.fromtimestamp(DEFAULT_UNIX_TIME)

			# get (formerly) living/unarchived threads to crawl them if we restarted the program
			cur.execute(get_alive_threads_sql, (board,))
			for tup in cur.fetchall():
				crawled_threads[board].add(tup[0])
				new_threads_to_crawl.add(tup[0])
		connection_pool.putconn(conn)
		headers["If-Modified-Since"] = convertDate(dt)
	
	
	response = requests.get(f"{CHAN_BASE_URL}/{board}/catalog.json", headers=headers)
	if response.status_code == 200:
		board_last_modified[board] = convertDate(datetime.fromtimestamp(int(time())))
		catalog = response.json()
		for page in catalog:
			for thread in page['threads']:
				if thread['no'] not in crawled_threads[board]:
					try:
						sticky = thread['sticky']
					except:
						sticky = 0
					try:
						closed = thread['closed']
					except:
						closed = 0
					crawled_threads[board].add(thread['no'])
					if not (closed or sticky):
						new_threads_to_crawl.add(thread['no'])
		
		with connection() as client: # faktory connection
			client.queue('chan_crawl_thread', queue='chan', args=(board, list(new_threads_to_crawl)))
	elif response.status_code == 304:
		logging.info(f"/{board}/catalog.json not modified since {headers['If-Modified-Since']}")
	else: 
		logging.info(f"Error {response.status_code} getting {board}/catalog.json")

def removeThread(board: str, thread: int):
	crawled_threads[board].discard(thread)

if __name__ == '__main__':
	# connection_pool in global scope. thread safe just in case
	connection_pool = ThreadedConnectionPool(1, 2, host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
	w = Worker(faktory=FAKTORY_URL, queues=['chan'], concurrency=1)
	w.register('chan_crawl_catalog', crawlCatalog)
	w.register('chan_remove_crawl', removeThread)
	logging.info("running 4chan catalog crawler?")
	w.run()
	connection_pool.closeall()
