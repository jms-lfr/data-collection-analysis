from tox_block.prediction import make_predictions
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
import os
from psycopg2.extras import execute_values
#from faktory import Worker
import logging
from time import sleep

load_dotenv()
FAKTORY_URL = os.environ.get('FAKTORY_URL')
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')


def main():
	conn = connection_pool.getconn()
	reddit_posts = getUnanalyzedRedditPosts(conn)
	chan_posts = getUnanalyzed4chanPosts(conn)

	# list comprehension to extract the comment from the posts to be analyzed
	reddit_analysis = [post[1] for post in reddit_posts]
	chan_analysis = [post[2] for post in chan_posts]
	
	# do analysis. store it in the same variable since we don't need 
	# the comments anymore. let the garbage collector do its magic.
	# these each become a dict of dicts with original comment + the following probabilities: 
	# 		toxic, severe_toxic, obscene, threat, insult, identity_hate
	# if either are empty lists, they stay empty lists (which are falsy)
	if reddit_analysis:
		reddit_analysis = make_predictions(reddit_analysis)
	if chan_analysis:
		chan_analysis = make_predictions(chan_analysis)

	with conn.cursor() as cur:
		# first, edit tuples
		for i in range(len(reddit_posts)):
			reddit_posts[i] = reddit_posts[i] + (reddit_analysis[i]['toxic'],) # comma to make it a tuple we can append
		for i in range(len(chan_posts)):
			chan_posts[i] = chan_posts[i] + (chan_analysis[i]['toxic'],)
		
		# then, update db
		if reddit_analysis:
			sql = """UPDATE reddit_posts 
					SET toxicity_rating = data.toxic 
					FROM (VALUES %s) 
					AS data (comment_id, comment, toxic) 
					WHERE 
						reddit_posts.comment_id = data.comment_id"""
			execute_values(cur, sql, reddit_posts)
		if chan_analysis:
			sql = """UPDATE chan_posts 
					SET toxicity_rating = data.toxic 
					FROM (VALUES %s) 
					AS data (board, post_id, post, toxic) 
					WHERE 
						chan_posts.board = data.board AND 
						chan_posts.post_id = data.post_id"""
			execute_values(cur, sql, chan_posts)

	conn.commit()
	connection_pool.putconn(conn)

def getUnanalyzedRedditPosts(conn) -> list:
	'''
		params: `conn` (database connection object returned by psycopg2)

		returns: List of tuples of reddit posts, containing comment_id and comment

		It is the caller's job to properly open/close the database connection.
	'''
	sql = """SELECT comment_id, comment 
			from reddit_posts 
			WHERE 
				toxicity_rating IS NULL AND
				comment NOT LIKE ''
			LIMIT 75"""
	with conn.cursor() as cur:
		cur.execute(sql)
		return cur.fetchall()

def getUnanalyzed4chanPosts(conn) -> list:
	'''
		params: `conn` (database connection object returned by psycopg2)

		returns: List of tuples of 4chan posts, containing board, post_id, and post

		It is the caller's job to properly open/close the database connection.
	'''
	sql = """SELECT board, post_id, post 
			from chan_posts 
			WHERE 
				toxicity_rating IS NULL AND
				post NOT LIKE ''
			LIMIT 75""" 
	with conn.cursor() as cur:
		cur.execute(sql)
		return cur.fetchall()


if __name__ == '__main__':
	connection_pool = SimpleConnectionPool(1, 2, host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
	#w = Worker(faktory=FAKTORY_URL, queues=['toxic'], concurrency=1)
	#w.register('toxicity_analyzer', main)
	logging.info("running toxicity analyzer?")
	
	while True:
		main()
		sleep(10)

	#w.run()
	connection_pool.closeall()
