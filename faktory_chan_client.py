import faktory 
from dotenv import load_dotenv
import os 
from time import sleep
from math import ceil

load_dotenv()
BOARDS = os.environ.get('BOARDS').split(',')
FAKTORY_URL = os.environ.get('FAKTORY_URL')

if __name__ == '__main__':
	board_length = len(BOARDS)
	chan_counter = 0
	with faktory.connection() as client:
		while True:
			board = BOARDS[chan_counter % board_length]
			client.queue('chan_crawl_catalog', queue='chan', args=(board,))
			chan_counter += 1
			sleep(ceil(30 / board_length)) # crawl each board every ~30 seconds (min 1 second wait)

