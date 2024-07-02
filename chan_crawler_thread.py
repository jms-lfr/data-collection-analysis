import requests
from dotenv import load_dotenv
import os
from datetime import datetime
import psycopg2
from psycopg2 import OperationalError
from psycopg2.pool import ThreadedConnectionPool
from lxml.html.clean import Cleaner
from lxml.html import document_fromstring
from time import sleep, time
from faktory import Worker, connection
import logging 
from random import randrange
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
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

def crawlThreads(board: str, threads: list, startSize: int = 50):
	dt = datetime.fromtimestamp(DEFAULT_UNIX_TIME)
	headers = {"If-Modified-Since": convertDate(dt), "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/{randrange(111, 120)}.0"}
	insert_sql = """INSERT INTO 
					chan_posts(timestamp, board, thread_id, post_id, post) 
					VALUES (%s, %s, %s, %s, %s) 
					ON CONFLICT (board, post_id) DO NOTHING"""
	update_sql = """UPDATE chan_posts 
					SET is_dead = '1' 
					WHERE 
						chan_posts.post_id = %s AND 
						chan_posts.board = %s AND 
						chan_posts.thread_id = 0""" # only spend time setting the OP to be dead
	
	if startSize < 20: # no start sizes smaller than 20
		startSize = 20
	
	threadHeaders = {}
	for thread in threads:
		threadHeaders[thread] = headers
	
	while True:
		for thread in threads[:]: # threads[:] == copy of threads[]
			url = f"{CHAN_BASE_URL}/{board}/thread/{thread}.json"
			response = requests.get(url, headers=threadHeaders[thread])
			if response.status_code == 200:
				threadHeaders[thread]["If-Modified-Since"] = convertDate(datetime.fromtimestamp(int(time())))
				thread_obj = response.json()['posts']
				
				conn = getDatabaseConnection()
				with conn.cursor() as cur: # add to db until conflict then stop
					for i in range(thread_obj[0]['replies'], -1, -1):
						post = thread_obj[i]
						try:
							comment = post['com']
						except:
							comment = ""
						insert = False
						if comment != "": # clean html (don't want it to affect toxicity analysis)
							insert = True
							cleaner = Cleaner(allow_tags=['br'], page_structure=False)
							comment = cleaner.clean_html(document_fromstring(comment)).text_content()
						elif post['resto'] == 0:
							# insert an empty comment IFF it is the original post of a thread
							insert = True
						
						if len(comment) > 2500:
							comment = comment[0:2500]
						if insert:
							cur.execute(insert_sql, (post['time'], board, post['resto'], post['no'], comment))
						
						if cur.rowcount is not None and cur.rowcount < 1 and comment != "": 
							# if comment == "", then we don't add it, 
							# 	so it's not safe to stop yet 
							# if cur.rowcount < 1, the row was already in the db, 
							# 	so anything before that is also in the db already,
							#	so we're done
							break
				try:
					archived = thread_obj[0]['archived']
				except:
					archived = 0
				try: 
					closed = thread_obj[0]['closed']
				except:
					closed = 0
				if archived or closed:
					cur.execute(update_sql, (thread, board))
					conn.commit()
					conn.close()
					with connection() as client: # faktory connection
						client.queue("chan_remove_crawl", queue='chan', args=(board, thread))
					logging.info(f"/{board}/thread/{thread} dead/archived, job complete")
					threads.remove(thread)
				conn.commit()
				conn.close()
			elif response.status_code == 304:
				if not randrange(0, 250): # 1 in 250 odds to log (arbitrary, just don't want it every time)
					logging.info(f"/{board}/thread/{thread} not modified since {headers['If-Modified-Since']}")
			elif response.status_code == 404:
				logging.info(f"{board}/thread/{thread} does not exist, stopping crawl")
				conn.close()
				with connection() as client: # faktory connection
					client.queue("chan_remove_crawl", queue='chan', args=(board, thread))
				threads.remove(thread)
			else:
				logging.info(f"Error {response.status_code} getting {board}/thread/{thread}")

		if len(threads) < (startSize / 2): #less than half as many threads as original = reset
			with connection() as client: # faktory connection
				# remove threads. if they're still in the catalog, they'll get crawled 
				# again, but hopefully in a process crawling more threads
				for thread in threads:
					client.queue("chan_remove_crawl", queue='chan', args=(board, thread))
			return
		sleep( max(1, 50/len(threads)) ) # fewer threads = sleep longer (want to crawl each thread every 50 seconds)

def newCrawlThreads(board: str, threads: list):
	'''
		params: `board` (name of 4chan board (in url)), `threads` (array of 4chan thread ids (as ints) in `board`)

		returns: none
		
		Creates new processes to crawl 30 /`board`/`thread`s, plus one to crawl
		any remaining threads. A process is ended and (ideally) recreated when there 
		are half as many threads to crawl as there were when it started.
	'''
	#process_pool.map(crawlThread, [board]*len(threads), threads)
	i = 0
	while (i+50) <= len(threads): # [x:y] is inclusive on x, exclusive on y 
		process_pool.submit(crawlThreads, board, threads[i:i+50])
		i += 50
	process_pool.submit(crawlThreads, board, threads[i:], len(threads[i:]))

def getDatabaseConnection():
	conn = None
	while conn is None:
		try: 
			conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
		except OperationalError:
			logging.info("Too many active db connections, waiting and trying again")
			sleep(4)
			conn = None
	return conn


if __name__ == '__main__':
	process_pool = ProcessPoolExecutor(max_workers=25, mp_context=get_context('spawn'))
	w = Worker(faktory=FAKTORY_URL, queues=['chan'], concurrency=1)
	w.register('chan_crawl_thread', newCrawlThreads)
	logging.info("running 4chan thread crawler?")
	w.run()
	process_pool.shutdown()
