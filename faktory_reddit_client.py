import faktory 
from dotenv import load_dotenv
import os 
from time import sleep
from datetime import datetime, timedelta

load_dotenv()
SUBREDDITS = os.environ.get('SUBREDDITS').split(',')
FAKTORY_URL = os.environ.get('FAKTORY_URL')

if __name__ == '__main__':
	subreddits_length = len(SUBREDDITS)
	reddit_counter = 0
	get_posts = True
	with faktory.connection() as client:
		client.queue('reddit_newkey', queue='reddit')
		last_token_update = datetime.utcnow()
		while True:
			if last_token_update < datetime.utcnow() + timedelta(hours=-24):
				client.queue('reddit_newkey', queue='reddit', priority=9)
				last_token_update = datetime.utcnow()
			subreddit = SUBREDDITS[reddit_counter % subreddits_length]
			reddit_counter += 1
			if subreddit != 'politics':
				client.queue('reddit_crawler', queue='reddit', args=(subreddit, get_posts, True))
			else:
				# get only posts for r/politics (part of CS415)
				client.queue('reddit_crawler', queue='reddit', args=('politics', True, False))

			# alternate between getting just comments and posts+comments 
			if (reddit_counter % subreddits_length) == 0:
				get_posts = not get_posts

			if get_posts:
				sleep(3)
			else:
				sleep(1.5)

