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

# Loads a json file
def load_json(filepath):
	with open(filepath) as json_file:
		return json.load(json_file)

# Command line: python twitter_kucoin_futures.py l (save trade logs) p (print query intervals)

# Load keys, keywords and users 
api_keys = load_json('keys.json')
users = load_json('users.json')
cryptos = load_json('keywords.json')

if 'prev_trades' in os.listdir():
	full_ex = False
else:
	full_ex = True

twitter_keys = {'consumer_key':api_keys['twitter_keys']['consumer_key'],'consumer_secret':api_keys['twitter_keys']['consumer_secret'],'access_token_key':api_keys['twitter_keys']['access_token_key'],'access_token_secret': api_keys['twitter_keys']['access_token_secret']}

# Use second group of twitter api keys
if '2' in sys.argv:	
	api_keys2 = load_json('../twitter_keys2.json')
	twitter_keys = {'consumer_key':api_keys2['twitter_keys']['consumer_key'],'consumer_secret':api_keys2['twitter_keys']['consumer_secret'],'access_token_key':api_keys2['twitter_keys']['access_token_key'],'access_token_secret': api_keys2['twitter_keys']['access_token_secret']}

# Get user inputs
print('\nEnter crypto to buy: '+'%s '* len(cryptos) % tuple(cryptos.keys()))
skip_input = False

# Buy currency
crypto  = input()
if not crypto:
	crypto = 'doge'
	skip_input = True
buy_coin = cryptos[crypto]

# Sell currency
if not skip_input:
	print('\nEnter currency to sell: '+'%s '* len(cryptos) % tuple(cryptos.keys()))
	sell_coin  = cryptos[input()]
else:
	sell_coin = cryptos['btc']

pair = [buy_coin['symbol'], sell_coin['symbol']]

# User to track
if not skip_input:
	print('\nUser: '+'%s '* len(users) % tuple(users.keys()))
	username = input()
	if username:
		user = users[username]
	else:
		user = users['bullet']
		skip_input = True
else:
	user = users['bullet']

# Time after buying before selling
hold_time = [1]
if not skip_input:
	print('\nHodl time(s) seconds e.g. 200 or 30,60,90: ')
	hold_time = input()
	if not hold_time:
		hold_time = [30,60,90]
	
	hold_time = ast.literal_eval('['+hold_time+']')
	print(hold_time)

print('\nHold time: '+'%.2fs '*len(hold_time) % hold_time)

# Amount of crypto to buy (Fastest if fewest queries before buying)
if not skip_input:
	print('\nAmount to buy in $: ')
	volume = input()
	if not volume:
		if crypto == 'doge':
			volume = 100
		elif crypto == 'btc':
			volume = 0.0002
	else:
		volume = float(volume)
else:
	volume = 50
print('\nVolume $%.8f' % volume)

# Simulation trade or real trade
simulate = True	
if not skip_input:
	print('\nTest y/n:')
	test = input()
	simulate = True
	if test == 'n': simulate = False

if simulate:
	print('\nSIMULATION TRADING')

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
