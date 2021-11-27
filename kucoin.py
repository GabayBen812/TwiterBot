import json
import time
from datetime import datetime
from typing import Any, Dict

import ccxt
import tweepy

import query

COIN_TO_SELL = "coin_to_sell"
SELL_PERCENTAGE = "sell_percentage"
AMOUNT_TO_BUY = "amount_to_buy"
KEYS = "keys"
USERS_TO_FIND = "users_to_find"
TRIGGERS = "triggers"
BLACKLIST = "blacklist"

TWITTER_KEYS = "twitter_keys"
CONSUMER_KEY = "consumer_key"
CONSUMER_SECRET = "consumer_secret"
ACCESS_TOKEN_KEY = "access_token_key"
ACCESS_TOKEN_SECRET = "access_token_secret"

KUCOIN_KEYS = "kucoin_keys"
CCTX_API_KEY = "apiKey"
CCTX_SECRET_KEY = "secret"
API_KEY = "api_key"
SECRET_KEY = "secret_key"

def _load_json(path: str) -> Dict[str, Any]:
    with open(path) as file:
        return json.load(file)

class Kucoin:

    def __init__(self, settings: str, cryptos: str, log: str=None):
        settings = _load_json(settings)

        self._coin_to_sell = settings[COIN_TO_SELL]
        self._sell_percentage = settings[SELL_PERCENTAGE]
        self._amount_to_buy = settings[AMOUNT_TO_BUY]
        self._api_keys = settings[KEYS]
        self._users = settings[USERS_TO_FIND]
        self._triggers = settings[TRIGGERS]
        self._blacklist = settings[BLACKLIST]
        self._auth = None
        self._api = None
        self._exchange = None

        self.refresh_exchange()

        if log:
            self._log = log

        # Ma ze???
        # if 'prev_trades' in os.listdir():
        #     full_ex = False
        # else:
        #     full_ex = True

        self._twitter_keys = {
            CONSUMER_KEY: self._api_keys[TWITTER_KEYS][CONSUMER_KEY],
            CONSUMER_SECRET: self._api_keys[TWITTER_KEYS][CONSUMER_SECRET],
            ACCESS_TOKEN_KEY: self._api_keys[TWITTER_KEYS][ACCESS_TOKEN_KEY],
            ACCESS_TOKEN_SECRET: self._api_keys[TWITTER_KEYS][ACCESS_TOKEN_SECRET]
        }

    @property
    def exchange(self) -> ccxt.kucoin:
        return self._exchange

    def auth_twitter(self):
        self._auth = tweepy.OAuthHandler(self._twitter_keys[CONSUMER_KEY], self._twitter_keys[CONSUMER_SECRET])
        self._auth.set_access_token(self._twitter_keys[ACCESS_TOKEN_KEY], self._twitter_keys[ACCESS_TOKEN_SECRET])
        self._api = tweepy.API(self._auth)

    def tweepy_pull(self, wait_tweet=True, print_timer=False, full_ex=True):
        # Query tweets from query.py file
        # In other words, run in the background and listen for tweets
        try:
            query.query_tweets(self._api, self, self._users, self._coin_to_sell, self._sell_percentage, self._amount_to_buy, wait_tweet, print_timer, full_ex=full_ex)
        except Exception as e:
            print(e)
            print('%s\n' % (datetime.now().strftime('%b %d - %H:%M:%S')))

    # Reset the exchange
    def refresh_exchange(self):
        self._exchange = ccxt.kucoin({CCTX_API_KEY: self._api_keys[KUCOIN_KEYS][API_KEY], CCTX_SECRET_KEY: self._api_keys[KUCOIN_KEYS][SECRET_KEY]})

    # Buying of real crypto
    def buy_crypto(self, ticker, buy_volume):
        # Try creating the buy order
        for i in range(10):
            try:
                buy_trade = self._exchange.create_order(ticker, 'market', 'buy', buy_volume)
                break
            except Exception as e:
                print(e)
                if i == 9:
                    print('Could not buy, exiting thread')
                    exit()
                print('\nBuy did not work, trying again')

        # Print buy
        try:
            if buy_trade.get('status') != 'open':
                avg_price = sum([float(x['price']) * float(x['qty']) for x in buy_trade['info']['fills']]) / sum(
                    [float(x['qty']) for x in buy_trade['info']['fills']])
                print('\nBought %s of %s at %s with %s %s of fees on %s\n' % (buy_trade['amount'] \
                                                                                  , buy_trade['symbol'], avg_price,
                                                                              buy_trade['fee']['cost'],
                                                                              buy_trade['fee']['currency'] \
                                                                                  , datetime.now().strftime(
                    '%b %d - %H:%M:%S')))
            else:
                print('\nBought %.8f at %s\n' % (buy_volume, datetime.now().strftime('%b %d - %H:%M:%S')))
        except Exception as e:
            print(e)
            print('\nError in print of buy')

        return buy_trade, buy_volume

    # Selling of real crypto
    def sell_crypto(self, ticker, buy_volume, buy_trade):

        # Try to sell 10 times
        for i in range(10):
            try:
                # Fees in USDT, Buy/Sell crypto amount is equal -- check if behaviour is the same when you have BNB in the wallet
                if ticker[-4:] == 'USDT':
                    sell_volume = buy_volume
                # If fees in the returned trade (filled on order)
                elif buy_trade['fee']:
                    if buy_trade['fee']['currency'] == 'BNB':
                        sell_volume = buy_volume
                    else:
                        # Converting fee currency to buy currency
                        ticker_pair = ticker.split('/')
                        if ticker_pair[0] != buy_trade['fee']['currency']:
                            fee_pair = [ticker_pair[0], buy_trade['fee']['currency']]
                            fee_ticker = '/'.join(fee_pair)
                            if fee_ticker not in tickers:
                                fee_ticker = '/'.join(fee_pair[::-1])
                            fee = self._exchange.fetchTicker(fee_ticker)
                            fee_price = (fee['bid'] + fee['ask']) / 2
                            sell_volume = buy_volume - fee_price * buy_trade['fee']['cost']

                        # When fee currency is the same as the buy currency
                        else:
                            sell_volume = buy_volume - buy_trade['fee']['cost']
                else:
                    sell_volume = buy_volume

                sell_trade = self._exchange.create_order(ticker, 'market', 'sell', sell_volume)
                break

            except Exception as e:
                error = e
                if 'MIN_NOTIONAL' in str(error):
                    buy_volume = buy_volume * 1.0005
                elif 'insufficient balance' in str(error):
                    buy_volume = buy_volume * 0.9995
                else:
                    self.refresh_exchange()
                print(e)
                print('\n\nTrying to sell %.10f again' % buy_volume)

        # Print sell
        if sell_trade['status'] != 'open':
            avg_price = sum([float(x['price']) * float(x['qty']) for x in sell_trade['info']['fills']]) / sum(
                [float(x['qty']) for x in sell_trade['info']['fills']])
            print('\nSold %s of %s at %s with %s %s of fees on %s' % (
                sell_trade['amount'], sell_trade['symbol'], avg_price \
                    , sell_trade['fee']['cost'], sell_trade['fee']['currency'],
                datetime.now().strftime('%b %d - %H:%M:%S')))
        else:
            print('\nSold %.8f at %s' % (sell_volume, datetime.now().strftime('%b %d - %H:%M:%S')))

        return sell_trade

    # Get data from self.exchange and print it
    def simulate_trade(self, buy, volume, ticker, conversion):
        if conversion[-4:] == 'USDT' and ticker[-4:] == 'USDT':
            usdpair = {'bid': 1, 'ask': 1}
        else:
            usdpair = self._exchange.fetchTicker(conversion)
        if buy:
            bid_ask, buy_sell = 'ask', 'Buying'
        else:
            bid_ask, buy_sell = 'bid', 'Selling'
        try:
            trade_price = self._exchange.fetchTicker(ticker)[bid_ask]
            price = (usdpair['bid'] + usdpair['ask']) / 2
            print('\n{} {} at {:.8f} {} = ${:.6f}'.format(buy_sell, volume, trade_price, ticker,
                                                          trade_price * volume * price))

        except Exception as e:
            print(e, '\nError in fetching ticker info')
        trade = {'symbol': ticker, 'side': 'buy' if buy else 'sell', 'amount': volume, 'cost': trade_price * volume}

        return trade

    # Summarise trade buy and sell
    def print_summary(self, simulate, ticker, buy_trade, sell_trades, conversion):

        if not simulate:
            buy_id, sell_ids = buy_trade['info']['orderId'], [i['info']['orderId'] for i in sell_trades]
            buy_prices, sell_prices = [], []
            for i in range(20):
                try:
                    trades = self._exchange.fetchMyTrades(ticker)
                    break
                except Exception as e:
                    print(e)
                    print("Couldn't fetch trades, tying again")

            # Loop over trades as one order could have had multiple fills
            for trade in trades[::-1]:
                if buy_id == trade['info']['orderId']:
                    buy_prices.append({'amount': trade['amount'], 'cost': trade['cost'], 'fee': trade['fee']})
                elif trade['info']['orderId'] in sell_ids:
                    sell_prices.append({'amount': trade['amount'], 'cost': trade['cost'],
                                        'fee': trade['fee']})  # Actual return uses fills

            buy_fee = sum([x['fee']['cost'] for x in buy_prices])
            sell_fee = sum([x['fee']['cost'] for x in sell_prices])

            # Log fees
            for i in range(20):
                try:
                    if buy_prices[0]['fee']['currency'] == 'BNB':
                        bnb_dollar = self._exchange.fetch_ticker('BNB/USDT')
                        bnb_price = (bnb_dollar['bid'] + bnb_dollar['ask']) / 2
                        buy_fee_dollar = buy_fee * bnb_price
                        if sell_prices[0]['fee']['currency'] == 'BNB':
                            sell_fee_dollar = sell_fee * bnb_price
                    elif buy_prices[0]['fee']['currency'] == 'USDT':
                        buy_fee_dollar = buy_fee
                        sell_fee_dollar = sell_fee
                    else:
                        buy_crypto_dollar = self._exchange.fetch_ticker(buy_prices[0]['fee']['currency'] + '/USDT')
                        sell_crypto_dollar = self._exchange.fetch_ticker(sell_prices[0]['fee']['currency'] + '/USDT')
                        buy_fee_price = (buy_crypto_dollar['bid'] + buy_crypto_dollar['ask']) / 2
                        sell_fee_price = (sell_crypto_dollar['bid'] + sell_crypto_dollar['ask']) / 2
                        buy_fee_dollar = buy_fee_price * buy_fee
                        sell_fee_dollar = sell_fee_price * sell_fee

                    ticker_pair = ticker.split('/')
                    if ticker_pair[1] == 'USDT':
                        ticker_info = {'bid': 1, 'ask': 1}
                    else:
                        ticker_info = self._exchange.fetch_ticker(ticker_pair[1] + '/' + 'USDT')
                    break
                except Exception as e:
                    print(e)
                    print('\nError in printing executed trades')
        else:
            sell_prices, buy_prices = sell_trades, [buy_trade]
            sell_fee_dollar, buy_fee_dollar = 0, 0
            if ticker[-4:] == 'USDT':
                ticker_info = {'bid': 1, 'ask': 1}
            else:
                ticker_info = self._exchange.fetch_ticker(ticker.split('/')[1] + '/' + 'USDT')

        buy_total = sum(buy_prices)
        sell_total = sum(sell_prices)
        avg_bid_ask = (ticker_info['bid'] + ticker_info['ask']) / 2

        gain_loss = (sell_total - buy_total) * avg_bid_ask - sell_fee_dollar - buy_fee_dollar
        gain_loss_percent = gain_loss / (buy_total * avg_bid_ask - sell_fee_dollar - buy_fee_dollar) * 100

        gain_text = '\nProfit/Loss: $%.6f   %.3f%%' % (gain_loss, gain_loss_percent)
        print(gain_text)

        return gain_text, buy_total, sell_total

    # Log the trade
    def log_trade(self, ticker, buy_volume, hold_times, buy_trade, sell_trades, gain_text, status, simulate):
        # Log trade
        now = datetime.now().strftime("%y-%m-%d_%H:%M:%S")

        # Saving name format: time_started, json_file_used, simluation/live
        with open("prev_trades/trades_%s_kucoin_%s_%s.txt" % (
                self.started_time.strftime('%Y-%m-%d_%H-%M-%S'), self.account_json,
                'simulation' if simulate else 'live'),
                  "a") as log_name:
            # If status is a dict, the message was from a web scrape
            if type(status) == dict:
                json.dump({'url': status['url'], 'update_text': status['update_text'],
                           'update_time': status['update_time'].strftime('%Y-%m-%d_%H:%M:%S'), 'ticker': ticker,
                           'hold_times': hold_times, 'complete_time': now, 'buy_volume': buy_volume,
                           'buy': buy_trade, 'sell': sell_trades, 'telegram': gain_text}, log_name)

            # If tweet from stream or query
            else:
                try:
                    full_text = status.text
                except:
                    full_text = status.full_text
                json.dump({'user': status.user.screen_name, 'tweet': full_text,
                           'tweet_time': status.created_at.strftime('%Y-%m-%d_%H:%M:%S'), 'ticker': ticker,
                           'hold_times': hold_times, 'complete_time': now, 'buy_volume': buy_volume,
                           'buy': buy_trade, 'sell': sell_trades, 'telegram': gain_text}, log_name)
            log_name.write('\n')

    # Execute trade
    def execute_trade(self, pair, hold_times=60, buy_volume=50, status=None):

        # Dealing with buy_sell volume pair or just a buy_volume
        if type(buy_volume) != list:
            sell_volumes = [buy_volume / len(hold_times) for _ in hold_times]
        else:
            sell_volumes = buy_volume[1]
            buy_volume = buy_volume[0]

        # Ticker and conversion
        ticker = pair[0] + '/' + pair[1]
        tousd1 = pair[0] + '/USDT'
        tousd2 = pair[1] + '/USDT'

        # If there is a block put on trading this ticker
        if self.block:
            if ticker in self.block_set:
                print('\nTrade of ' + ticker + ' blocked in ' + str(self.block_set))
                return
            # When bought add and blocker flag set
            self.block_set.add(ticker)
            print('Added to blockset ' + str(self.block_set))

        # Buy order
        buy_trade, buy_volume = self.buy_crypto(ticker, buy_volume)

        # Sell in multiple stages based on hold_times
        prev_sell_time = 0
        sell_trades = []
        for hold, sell_volume in zip(hold_times, sell_volumes):
            time.sleep(hold - prev_sell_time)
            prev_sell_time = hold

            # Sell order
            sell_trades.append(self.sell_crypto(ticker, sell_volume, buy_trade))

        # Remove block when trade finishes
        if self.block:
            self.block_set.remove(ticker)
            print('Removing %s from block set' % (ticker))

        print('\n\nTRADE FINISHED\n')

        # Print summary and log
        try:
            gain_text, buy_total, sell_total = self.print_summary(simulate, ticker, buy_trade, sell_trades, tousd2)
        except Exception as e:
            print('\nFailed to print summary\n')
            print(e)

        # Log trade
        if self.logfile:
            self.log_trade(ticker, buy_volume, hold_times, buy_trade, sell_trades, gain_text, status, simulate)