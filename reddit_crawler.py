import requests
import requests.auth
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_values
from time import sleep
from faktory import Worker
import logging


load_dotenv()
REDDIT_API_KEY = os.environ.get('REDDIT_API_KEY')
REDDIT_CLIENT_ID = os.environ.get('REDDIT_CLIENT_ID')
REDDIT_SECRET = os.environ.get('REDDIT_SECRET')
REDDIT_USER = os.environ.get('REDDIT_USER')
REDDIT_PASS = os.environ.get('REDDIT_PASS')

USER_AGENT = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0 (by /u/{REDDIT_USER})"

FAKTORY_URL = os.environ.get('FAKTORY_URL')
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')

HEADERS = {"Authorization": f"bearer {REDDIT_API_KEY}", "User-Agent": USER_AGENT}
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

REDDIT_BASE_URL = """https://oauth.reddit.com"""
#SUBREDDITS = os.environ.get('SUBREDDITS').split(',')
SLEEP_LENGTH = 1.2

def main(subreddit: str, get_posts: bool, get_comments: bool):
	conn = connection_pool.getconn()
	if get_posts:
		# get posts AND comments 
		# in general, more comments are made than original posts, so we want to get comments more often
		
		#first, get posts
		new_posts_endpoint = f"r/{subreddit}/new.json?sort_new&limit=100"
		response = requests.get(f"{REDDIT_BASE_URL}/{new_posts_endpoint}", headers=HEADERS)
		if response.status_code == 200:
			all_data = response.json()['data']
			last_index_to_add = binarySearch(all_data['children'], all_data['dist'], subreddit, conn)
			data_tuples = []
			for i in range(last_index_to_add, -1, -1):
				post = all_data['children'][i]['data']
				body = post['title'] + "\n\n" + post['selftext']
				if len(body) > 2500:
					body = body[0:2500]
				if body:
					data_tuples.append((int(post['created_utc']),
										subreddit,
										post['name'], # already has t3_ prefix
										post['name'],
										body))
			
			#print(f"{data_tuples}")
			sql = """INSERT INTO reddit_posts(timestamp, subreddit, post_id, comment_id, comment) VALUES %s 
					ON CONFLICT (comment_id) DO NOTHING"""
			with conn.cursor() as cur:
				execute_values(cur, sql, data_tuples)
				conn.commit()
		elif response.status_code == 401:
			logging.info("Need new OAuth key")
			#logging.info(f"Old key: {REDDIT_API_KEY}")
			updateOAuthKey()
			#logging.info(f"New key: {REDDIT_API_KEY}")
		else:
			logging.info(f"Error {response.status_code}: Failed to get reddit posts for {subreddit}")
			sleep(SLEEP_LENGTH)
		
		sleep(SLEEP_LENGTH) 
	
	if get_comments:
		getCommentsAndAddToDB(subreddit, conn) 
	
	connection_pool.putconn(conn)

def binarySearch(arr, size: int, subreddit: str,  conn) -> int:
	'''
		params: `arr` (list of reddit posts/comments as dicts); `size` (int, size of the list); 
		`subreddit` (string, subreddit posts/comments are from); `conn` (database connection object returned by psycopg2)

		returns: int (index of the last element of the list to ADD to the db, 
		i.e. the last element of the list that is NOT IN the db)

		Binary searches the given list to find the last element of the list that is not already in the database.  
		Caller should then add the new content to the database. It is also the caller's job to properly 
		open and close the database connection. 
	'''
	def isInDB(comment_id: str, conn) -> bool:
		'''
			params:`comment_id` (str, key for reddit_posts table), `conn` (database connection object returned by psycopg2)

			returns: bool (True if the row is in the table)

			Checks the reddit_posts table for the row. Abstracted away for readability/simplicity.
		'''
		with conn.cursor() as cur:
			sql = """SELECT EXISTS(SELECT 1 FROM reddit_posts 
					WHERE comment_id=%s 
					LIMIT 1)"""
			cur.execute(sql, (comment_id,)) 
			return cur.fetchone()[0]
	
	max_timestamp = 0
	with conn.cursor() as cur:
		cur.execute("""SELECT MAX(timestamp) FROM reddit_posts WHERE subreddit=%s""", (subreddit,))
		max_timestamp = cur.fetchone()[0] if cur.rowcount > 0 else 0
	if max_timestamp is None:
		max_timestamp = 0
	left = 0
	right = size - 1
	middle = 0
	curr_post = None
	while left < right:
		middle = left + ((right - left) // 2)
		curr_post = arr[middle]['data']
		post_timestamp = int(curr_post['created_utc'])
		if post_timestamp > max_timestamp or (post_timestamp == max_timestamp and not isInDB(curr_post['name'], conn)): 
			left = middle + 1
		else: 
			right = middle - 1
	curr_post = arr[left]['data']
	return left - 1 if isInDB(curr_post['name'], conn) else left


def getCommentsAndAddToDB(subreddit: str, conn) -> bool:
	'''
		params: `subreddit` (str) (subreddit to get comments from); `conn` (database connection object returned by psycopg2)

		returns: bool (successfully added to db, or failed to add)

		Gets comments from a subreddit and adds them to the database connected to by `conn`.
		This function will create and close a cursor for interacting with the database. 
		It is the caller's job to properly open/close the database connection and to
		respect Reddit's API rate limits. Function commits the modifications.
	'''
	comments_endpoint = f"r/{subreddit}/comments.json?sort_new&limit=100"
	response = requests.get(f"{REDDIT_BASE_URL}/{comments_endpoint}", headers=HEADERS)
	if response.status_code == 200:
		all_data = response.json()['data']
		last_index_to_add = binarySearch(all_data['children'], all_data['dist'], subreddit, conn)
		data_tuples = []
		for i in range(last_index_to_add, -1, -1): # reverse order so max(created_utc) should be last in db
			
			comment = all_data['children'][i]['data']
			body = comment['body']
			if len(body) > 2500:
				body = body[0:2500]
			if body:
				data_tuples.append((int(comment['created_utc']), 
									subreddit, 
									comment['link_id'], # already has t3_ prefix
									comment['name'], # already has t1_ prefix
									body))

		sql = """INSERT INTO reddit_posts(timestamp, subreddit, post_id, comment_id, comment) VALUES %s 
				ON CONFLICT (comment_id) DO NOTHING"""
		with conn.cursor() as cur:
			execute_values(cur, sql, data_tuples)
			conn.commit()

		return True
	elif response.status_code == 401:
		updateOAuthKey()
	else:
		logging.info(f"Error {response.status_code}: Failed to get reddit comments for {subreddit}")
		sleep(SLEEP_LENGTH)
		return False

def updateOAuthKey():
	sleep(SLEEP_LENGTH)
	client_auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_SECRET)
	post_data = {"grant_type": "password", "username": REDDIT_USER, "password": {REDDIT_PASS}}
	headers = {"User-Agent": USER_AGENT}
	response = requests.post("https://www.reddit.com/api/v1/access_token", auth=client_auth, data=post_data, headers=headers)
	global REDDIT_API_KEY 
	REDDIT_API_KEY = response.json()['access_token']
	HEADERS['Authorization'] = f"bearer {REDDIT_API_KEY}"
	sleep(SLEEP_LENGTH)

if __name__ == '__main__':
	connection_pool = SimpleConnectionPool(1, 4, host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
	w = Worker(faktory=FAKTORY_URL, queues=['reddit'], concurrency=1)
	w.register('reddit_crawler', main)
	w.register('reddit_newkey', updateOAuthKey)
	logging.info("running reddit?")
	w.run()
	connection_pool.closeall()
