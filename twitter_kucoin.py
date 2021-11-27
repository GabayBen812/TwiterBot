import tweepy
import json
import time
from datetime import datetime
from kucoin_api import *
from query import *
import ast
import traceback

# Checks if a tweet from a user contains a particular trigger word
def tweepy_pull(api, user, pair, crypto, hold_time, volume, simulate, wait_tweet=True, logfile=None, print_timer=False, full_ex=True):

	exchange = kucoin_api(api_keys, logfile=logfile)

	# Query tweets from query.py file
	try:
		query_tweets(api, exchange, user, pair, crypto, hold_time, volume, simulate, wait_tweet, print_timer,
					 full_ex=full_ex)
	except Exception as e:
		print(traceback.print_exc())
		print(e)
		print('%s\n' % (datetime.now().strftime('%b %d - %H:%M:%S')))

# Command line: python twitter_kucoin_futures.py -log (save trade logs) -queries (print query intervals)

# Inintilizing a file of jsons to log trades
logfile = False
if '-log' in sys.argv:
	logfile = True

# Use twitter API
auth = tweepy.OAuthHandler(twitter_keys['consumer_key'], twitter_keys['consumer_secret'])
auth.set_access_token(twitter_keys['access_token_key'], twitter_keys['access_token_secret'])
api = tweepy.API(auth)

# Execute function
tweepy_pull(api, user, pair, buy_coin, hold_time, volume, simulate, wait_tweet=not skip_input, logfile=logfile, full_ex=full_ex)
