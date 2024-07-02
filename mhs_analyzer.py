import requests
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
from time import sleep
from json import JSONEncoder
import threading
import logging
from sys import exit

load_dotenv()
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')

MHS_API_KEY = os.environ.get('MHS_API_KEY')
HEADERS = {"Content-Type": "application/json"}
MHS_BASE_URL = "https://api.moderatehatespeech.com/api/v1/moderate/"
REDDIT_UPDATE_SQL = """UPDATE reddit_posts 
					SET
						mhs_class = data.class, 
						mhs_confidence = data.confidence 
					FROM (VALUES %s) 
					AS data (comment_id, comment, class, confidence) 
					WHERE 
						reddit_posts.comment_id = data.comment_id"""
CHAN_UPDATE_SQL = """UPDATE chan_posts 
				SET 
					mhs_class = data.class, 
					mhs_confidence = data.confidence 
				FROM (VALUES %s) 
				AS data (board, post_id, post, class, confidence) 
				WHERE 
					chan_posts.board = data.board AND 
					chan_posts.post_id = data.post_id"""

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

def main(reddit: bool):
	'''
		params: `reddit` (bool, analyze reddit posts (True) or 4chan posts (False)?) 

		returns: none/never (infinite loop)

		Gets newest posts/comments from PostgreSQL database, makes POST requests 
		to the ModerateHateSpeech (MHS) API, and updates the database with the 
		analysis done by MHS.
	'''
	if reddit:
		getposts = getUnanalyzedRedditPosts
		update_sql = REDDIT_UPDATE_SQL
	else:
		getposts = getUnanalyzed4chanPosts
		update_sql = CHAN_UPDATE_SQL
	
	while True:
		conn = connection_pool.getconn()
		posts = getposts(conn)
		connection_pool.putconn(conn)

		if posts:
			for i in range(len(posts)):
				# MHS is lame so we have to make it a json string
				post_data = JSONEncoder().encode({"token": MHS_API_KEY, "text": posts[i][1 if reddit else 2]})
				
				try:
					response = requests.post(MHS_BASE_URL, headers=HEADERS, data=post_data)
					if response.status_code == 200:
						response_data = response.json()
						#print(response_data)
						if response_data['response'] == 'Success':
							posts[i] = posts[i] + (response_data['class'], float(response_data['confidence']),)
						else:
							# we got a fail response, so assume post can 
							# never be analyzed and make its mhs_class non-null
							posts[i] = posts[i] + ('N/A', None)
					elif response.status_code >= 500:
						logging.info(f"MHS might be down (HTTP {response.status_code}), sleeping for 3 mins")
						posts[i] = posts[i] + (None, None)
						sleep(60*3)
					else:
						logging.info(f"MHS returned status code {response.status_code} for \'{posts[i][1 if reddit else 2]}\'")
						posts[i] = posts[i] + (None, None)
						
				except KeyError as e: 
					try:
						error_msg = response_data['error']
						logging.info(f"MHS Error: \'{error_msg}\' for \'{posts[i][1 if reddit else 2]}\'")
						posts[i] = posts[i] + ('ERROR', None)
						continue
					except KeyError:
						pass
					logging.info(f"MHS KeyError accessing {e}: {response_data}")
					posts[i] = posts[i] + (None, None)
				except Exception as e: # no response (e.g. connection timeout)
					logging.exception(f"MHS Error!")
					posts[i] = posts[i] + (None, None)
			
			conn = connection_pool.getconn()
			with conn.cursor() as cur:
				execute_values(cur, update_sql, posts)
			
			conn.commit()
			connection_pool.putconn(conn)
		else:
			# we had no posts to analyze, so wait for a while for some to accumulate
			sleep(10)


def getUnanalyzedRedditPosts(conn) -> list:
	'''
		params: `conn` (database connection object returned by psycopg2)

		returns: List of tuples of *most recent* unanalyzed reddit posts, containing comment_id and comment

		It is the caller's job to properly open/close the database connection.
	'''
	sql = """SELECT comment_id, comment 
			from reddit_posts 
			WHERE 
				mhs_class IS NULL 
				AND comment NOT LIKE ''
			ORDER BY timestamp DESC
			LIMIT 100"""
	with conn.cursor() as cur:
		cur.execute(sql)
		return cur.fetchall()
	
def getUnanalyzed4chanPosts(conn) -> list:
	'''
		params: `conn` (database connection object returned by psycopg2)

		returns: List of tuples of *most recent* unanalyzed 4chan posts, containing board, post_id, and post

		It is the caller's job to properly open/close the database connection.
	'''
	sql = """SELECT board, post_id, post 
			from chan_posts 
			WHERE 
				mhs_class IS NULL AND
				post NOT LIKE ''
			ORDER BY timestamp DESC
			LIMIT 100""" 
	with conn.cursor() as cur:
		cur.execute(sql)
		return cur.fetchall()

if __name__ == '__main__':
	connection_pool = ThreadedConnectionPool(1, 3, host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
	reddit_thread = threading.Thread(target=main, args=(True,), daemon=True)
	chan_thread = threading.Thread(target=main, args=(False,), daemon=True)
	reddit_thread.start()
	chan_thread.start()

	logging.info("MHS threads have started")

	try:
		reddit_thread.join()
		chan_thread.join()
	except KeyboardInterrupt:
		connection_pool.closeall()
		exit(130) # Linux exit code when terminating with Ctrl-C
	
	connection_pool.closeall()
