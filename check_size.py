# part of CS415

import psycopg2
from dotenv import load_dotenv
import os
import datetime

load_dotenv()
FILENAME = os.environ.get('DATA_COLLECTION_AMOUNT_FILE')
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')

def main():
	sql = """SELECT pg_total_relation_size('{}')"""
	
	conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
	cur = conn.cursor()

	cur.execute(sql.format("reddit_posts"))
	redditsize = cur.fetchone()[0]

	cur.execute(sql.format("chan_posts"))
	chansize = cur.fetchone()[0]

	with open(FILENAME, "a") as file:
		file.write(f"{datetime.datetime.now()}: { float(redditsize+chansize) / (1024*1024)} MB\n")
	
	conn.close()

if __name__ == '__main__':
	main()
