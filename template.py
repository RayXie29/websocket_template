import time
import yaml
import ccxt
import asyncio
import argparse
from collections import deque, defaultdict
from pybit.unified_trading import WebSocket

from config import bybitConfig
from MarketMaking.strategies.bybitStrategy import STRATEGY

private_url = "wss://stream.bybit.com/v5/private"
public_url = "wss://stream.bybit.com/v5/public/spot"

#NOT MUST DO
class RestfulAPI:
    def __init__(self, api_key, api_secret):

        self.api_key = api_key
        self.api_secret = api_secret

        self.exchange = ccxt.bybit({
            'apiKey' : api_key,
            'secret' : api_secret
        })

        self.exchange.options['defaultType'] = 'spot'

    def get_public_order_book(self, pair, limit=20):
        try:
            return self.exchange.fetch_order_book(pair, limit)
        except Exception as e:
            print('get public order book failed because {}...return empty dictionary'.format(e))
            return {}

    def get_public_recent_trades(self, pair, limit=20):
        try:
            return self.exchange.fetch_trades(symbol=pair, limit=limit)
        except Exception as e:
            print('get public recent trades failed because {}...return empty dictionary'.format(e))
            return {}
    def get_private_account_balanceAll(self, purseType='SPTP'):
        try:
            return self.exchange.fetch_balance()['info']['result']['balances']
        except Exception as e:
            print('get private account balanceAll failed because {}...return empty dictionary'.format(e))
            return {}

    def get_private_order_list(self, pair, limit=None, orderId=None):
        params = {}
        if limit is not None:
            params.update({"limit" : int(limit)})
        if orderId is not None:
            params.update({"orderId": orderId})

        try:
            return self.exchange.fetch_open_orders(symbol=pair, params=params)
        except Exception as e:
            print('get private order list failed because {}...return empty dictionary'.format(e))
            return {}
            
    def get_private_closed_order_list(self, pair, limit=None, orderId=None, sttime=None, edtime=None):
        params = {}
        if limit is not None:
            params.update({"limit":limit})
        if orderId is not None:
            params.update({"orderId":orderId})
        if sttime is not None:
            params.update({"startTime":sttime})
        if edtime is not None:
            params.update({"endTime":edtime})
        
        try:
            return self.exchange.fetch_closed_orders(symbol=pair, params=params)
        except Exception as e:
            print('get private closed order list failed because {}...return empty dictionary'.format(e))
            return {}

    def set_private_cancel_order(self, _id, pair):
        try:
            return self.exchange.cancel_order(_id, pair)
        except Exception as e:
            print('set private cancel order failed because {}...return empty dictionary'.format(e))
            return {}
    
    def set_private_cancel_all_orders(self, pair):
        try:
            return self.exchange.cancel_all_spot_orders(symbol=pair)
        except Exception as e:
            print("set private cancel all {} spot orders failed because {}...return empty dictionary".format(pair, e))


    def set_private_create_order(self, pair, action, amount, price, stop=-1, _type='limit', clOrderID=''):
        try:
            return self.exchange.create_order(pair, _type, action, amount, price)
        except Exception as e:
            print('set private create order failed because {}...return empty dictionary'.format(e))
            return {}



#MustDO
class OrderBookHandler:
    def __init__(self, api, pairs, depth, ignore_small_order_notional=None):
        """
        Object to handle the coin market books, which are bids/asks information
        NOTE: The data receive from exchanges might have different format. EX: value keys, data structure...
        inp_args:
            api : restful api
            pairs : coin pairs
            depth : maximum depth of books would like to record
            ignore_small_order_notional : flag for ignoring small order, not implemented
        """
        self.api = api
        self.pairs = pairs
        self.depth = depth
        self.ignore_small_order_notional = ignore_small_order_notional

        self.bids = {}
        self.asks = {}
        self.bidsizes = {}
        self.asksizes = {}
        self.spreads = {}
        self.reset()
        self._initialization()

    #NOT MUST DO
    def _initialization(self):
        """

        initialize function to fill the bids / asks information through restful api
        """
        for pair in self.pairs:
            quote = self.api.get_public_order_book(pair=pair, limit=20)
            bids = quote['bids']
            asks = quote['asks']
            currentBidDepth = min(self.depth, len(bids))
            currentAskDepth = min(self.depth, len(asks))

            self.bids[pair] = [
                float(bids[d][0]) for d in range(currentBidDepth)
            ]
            self.asks[pair] = [
                float(asks[d][0]) for d in range(currentAskDepth)
            ]

            self.bidsizes[pair] = [
                float(bids[d][1]) for d in range(currentBidDepth)
            ]

            self.asksizes[pair] = [
                float(asks[d][1]) for d in range(currentAskDepth)
            ]

            if self._check_valid(pair) > 0:
                self.spreads[pair] = self._calculate_spread(pair)

    def _check_valid(self, pair):
        """
        function for checking if there are empty bids or asks
        inp_args:
            pair : coin pair
        out_args:
            flag of whether there are empty bids or asks
        """
        if len(self.bids[pair]) > 0 and len(self.asks[pair]) > 0:
            return True
        return False

    def _calculate_spread(self, pair):
        """
        function for calculating the spread of certain coin pair
        inp_args:
            pair : coin pair
        out_args:
            spread of certain coin pair
        """
        bid1 = self.get_bid1(pair)
        ask1 = self.get_ask1(pair)
        if bid1 == 0 or ask1 == 0 or ask1 < bid1:
            return 0
        return (ask1 - bid1) / bid1

    def reset(self):
        """
        function for reset all the bids / asks / spreads information
        """
        self.bids = {pair: [] for pair in self.pairs}
        self.asks = {pair: [] for pair in self.pairs}
        self.spreads = {pair: 0 for pair in self.pairs}

    def update(self, msg):
        """
        function for update the realtime order books of coin pairs 
        inp_args:
            msg : data received from exchange
        """
        if msg:
            pair = msg['data']['s']
            if pair in self.pairs:

                #Making sure the bids/asks is sorted in descending/ascending order
                #In case the order prices received from exchange are not sorted
                bids = sorted([bid for bid in msg['data']['b']],
                              key=lambda x: x[0],
                              reverse=True)
                
                asks = sorted([ask for ask in msg['data']['a']],
                              key=lambda x: x[0])

                currentBidDepth = min(len(bids), self.depth)
                currentAskDepth = min(len(asks), self.depth)

                #Fill the bids / asks informtaion from exchange data
                self.bids[pair] = [
                    float(bids[d][0]) for d in range(currentBidDepth)
                ]
                self.bidsizes[pair] = [
                    float(bids[d][1]) for d in range(currentBidDepth)
                ]
                self.asks[pair] = [
                    float(asks[d][0]) for d in range(currentAskDepth)
                ]
                self.asksizes[pair] = [
                    float(asks[d][1]) for d in range(currentAskDepth)
                ]

                if self._check_valid(pair):
                    self.spreads[pair] = self._calculate_spread(pair)

    def get_spread(self, pair):
        """
        Function for return the spread of certain coin pair
        inp_args:
            pair : coin pair
        out_args:
            spread of certain coin pair
        """
        return self.spreads[pair]

    def get_bid1(self, pair):
        """
        Function for return bid1 price of certain coin pair
        inp_args:
            pair : coin pair
        out_args:
            bid1 price of certain coin pair
        """
        if len(self.bids) > 0:
            return self.bids[pair][0]
        return 0

    def get_ask1(self, pair):
        """
        Function for return ask1 price of certain coin pair
        inp_args:
            pair : coin pair
        out_args:
            ask1 price of certain coin pair
        """
        if len(self.asks) > 0:
            return self.asks[pair][0]
        return 0

    def get_bids(self, pair):
        """
        Function for return bids information of certain coin pair
        inp_args:
            pair : coin pair
        out_args:
            bids information of certain coin pair
        """
        if len(self.bids) > 0:
            return self.bids[pair], self.bidsizes[pair]
        return [0 for _ in range(self.depth)], [0 for _ in range(self.depth)]

    def get_asks(self, pair):
        """
        Function for return asks information of certain coin pair
        inp_args:
            pair : coin pair
        out_args:
            asks information of certain coin pair
        """
        if len(self.asks) > 0:
            return self.asks[pair], self.asksizes[pair]
        return [0 for _ in range(self.depth)], [0 for _ in range(self.depth)]

    def display(self):
        """
        Debug function
        """
        for pair in self.pairs:
            if self._check_valid(pair):
                print(
                    f"{pair} : bid1 : {self.get_bid1(pair)}, ask1 : {self.get_ask1(pair)}, spread : {self.spreads[pair]}"
                )

#MustDO
class TradeHandler:

    def __init__(self, pairs, max_record_amount=100):
        """
        Object for recording the traded orders of coin pairs, which means orders been filled
        NOTE: The data receive from exchanges might have different format. EX: value keys, data structure...
        inp_args:
            pairs : coin pairs
            max_record_amount : how much traded information would like to record --> for saving memory
        """
        self.pairs = pairs
        self.max_record_amount = max_record_amount
        self.prices = {}
        self.sizes = {}
        self.ts = {}
        self.directions = {}
        self.reset()

    def reset(self):
        """
        Function for reset the information dictionary
        All the information is recorded in deque, it is much easier to remove the oldest trade and insert newest trade
        
        prices : traded prices of certain coin pair
        sizes : traded sizes of certain coin pair
        ts : traded timestamp of certain coin pair
        directions : traded direction of certain coin pair -> buy or sell
        NOTE: This 4 deque must have same length all the time
        """
        self.prices = {
            pair: deque(maxlen=self.max_record_amount)
            for pair in self.pairs
        }
        self.sizes = {
            pair: deque(maxlen=self.max_record_amount)
            for pair in self.pairs
        }
        self.ts = {
            pair: deque(maxlen=self.max_record_amount)
            for pair in self.pairs
        }
        self.directions = {
            pair: deque(maxlen=self.max_record_amount)
            for pair in self.pairs
        }

    def update(self, msg):
        """
        Function for update the traded information of certain coin pair
        """
        if msg:
            trades = msg['data']
            for trade in trades:
                current_pair = trade['s']
                if current_pair in self.pairs:
                    self.prices[current_pair].append(float(trade["p"]))
                    self.sizes[current_pair].append(float(trade["v"]))
                    #Exchanges might return different unit of timestamp
                    #Please make sure the record them in 10 digits, which is second unit
                    #From example, bybit return 13 digits -> ms unit
                    #So divide the timestamp as 10e3 before record
                    self.ts[current_pair].append(trade["T"] / 1e3)
                    #Exchanges might return different flag of traded dirctions
                    #For example, bybit simply return buy or sell the indicate the direction
                    #But some other exchange might simply return 0 for buy 1 for sell
                    #Please make sure the directions is recorded with string 'BUY' or 'SELL'
                    self.directions[current_pair].append(trade["S"].upper())

    def get_most_recent_price(self, pair):
        """
        Function for retrieving the most recent traded price of certain coin pair
        inp_args:
            pair : coin pair
        out_args:
            Most traded price of certain coin pair
        """
        if len(self.prices[pair]) > 0:
            return self.prices[pair][-1]
        return 0

    def get_side_trades(self, pair, side, size_limit, time_limit):
        """
        Function for retrieving traded record of single side(buy or sell), during
        certain time(like within 1800 seconds) and with certain size record(like only 20 traded record)
        inp_args:
            pair : coin pair
            side : which side of traded record, buy or sell
            size_limit : maximum traded record would like to get
            time_limit : maximum peroid of traded record would like to get
        out_args:
            prices and sizes of traded records
        """
        prices = deque()
        sizes = deque()
        now = time.time()

        for idx in range(len(self.prices[pair]) - 1, -1, -1):
            if len(prices) >= size_limit: break
            if self.directions[pair][
                    idx] == side and now - self.ts[pair][idx] <= time_limit:
                prices.appendleft(self.prices[pair][idx])
                sizes.appendleft(self.sizes[pair][idx])

        return list(prices), list(sizes)

    def _check_valid(self, pair):
        """
        Function for checking if traded record of certain coin pair is empty
        inp_args:
            pair : coin pair
        out_args:
            flag of whether the traded record of certain coin pair is empty
        """
        if len(self.prices[pair]) != 0:
            return True
        return False

    def display(self):
        """
        Debug function
        """
        for pair in self.pairs:
            if self._check_valid(pair):
                print(
                    f"{pair} : trade buy price : {self.get_side_trades(pair, 'BUY', 20, 1e9)}"
                )
                print(
                    f"{pair} : trade sell price : {self.get_side_trades(pair, 'SELL', 20, 1e9)}"
                )

#MustDO
class WalletHander:

    def __init__(self, api, accountType='SPOT'):
        """
        Object for recording the account wallet information, like how many btc is left or how many usdt is left
        NOTE: The data receive from exchanges might have different format. EX: value keys, data structure...
        inp_args:
            api : restful api from certain exchange
            accountType : Only spot(現貨) information is needed, exchanges 
                          should return the message depends on account type
                          For example: 現貨, 合約 blablabla...
        """
        self.api = api
        self.accountType = accountType.lower()
        self.positions = defaultdict(float)
        self.frees = defaultdict(float)
        self._initialize()

    #NOT MUST DO
    def _initialize(self):
        """
        Initialize function for fill in the current account information of spot(現貨)
        Since websocket only return the information which has been changed, for 
        example, if I sold 0.3 btc, then websocket will only update decreasing of btc and increasing of usdt
        """
        accountBalance = self.api.get_private_account_balanceAll()
        if len(accountBalance) > 0:
            for coin_info in accountBalance:
                self.positions[coin_info['coin']] = float(coin_info['total'])
                self.frees[coin_info['coin']] = float(coin_info['free'])

    def update(self, msg):
        """
        Function for updating the account information of certain account type -> spot(現貨)
        The information returned from exchange might have different data structure
        inp_args:
            msg : websocket message from exchange
        """
        if msg:
            data = msg['data']
            for single_data in data:
                if single_data['accountType'].lower() == self.accountType:
                    for coin_info in single_data['coin']:
                        self.positions[coin_info['coin']] = float(
                            coin_info['walletBalance'])
                        self.frees[coin_info['coin']] = float(
                            coin_info['free'])

    def get_position(self, coin):
        """
        Function for getting how many position left in wallect of certain coin pair
        inp_args:
            coin : certain coin pair
        out_args:
            position of certain coin pair
        """
        if coin in self.positions:
            return self.positions[coin]
        return 0

    def get_free_position(self, coin):
        """
        Function for getting how many free position left in wallet of certain coin pair
        For example, the original position of btc in my wallet is 1.0, but I placed a
        sell order on price 33000 with size 0.5, then only 0.5 free position left in wallet
        inp_args:
            coin : certain coin pair 
        """
        if coin in self.frees:
            return self.frees[coin]
        return 0

    def display(self):
        """
        Debug function
        """
        print(f"{self.accountType} wallet info:")
        print(f"Positions : {self.positions}")
        print(f"Frees : {self.frees}")

#MustDO
class OrderHandler:

    def __init__(self, pairs, api, accountType='spot'):
        """
        Object for recording the open and closed orders of certain coin pair
        open ordrs -> orders have not been filled
        closed orders -> orders have been filled
        NOTE: The data receive from exchanges might have different format. EX: value keys, data structure...
        inp_args:
            pairs : coin pairs
            api : restful api of certain exchange
            accountType : Only spot(現貨) information is needed, exchanges 
                          should return the message depends on account type
                          For example: 現貨, 合約 blablabla...
        """
        self.pairs = pairs
        self.api = api
        self.accountType = accountType.lower()
        self.open_buy_orders = defaultdict(dict)
        self.open_sell_orders = defaultdict(dict)
        self.closed_buy_orders = defaultdict(dict)
        self.closed_sell_orders = defaultdict(dict)
        
        self._initialize()
        
    #NOT MUST DO
    def _initialize(self):
        """
        Initialize function by using restful api to fill in current open/closed orders
        """
        def gather_orders(orders, buy_order_dict, sell_order_dict, with_ts=False):
            buy_orders = [
                order for order in orders if order['side'] == 'buy'
            ]
            sell_orders = [
                order for order in orders if order['side'] == 'sell'
            ]

            for buy_order in buy_orders:
                buy_order_dict[pair][float(buy_order['price'])] = {
                    'qty': float(buy_order['amount']),
                    'order_id': buy_order['id'],
                }
                if with_ts:
                    buy_order_dict[pair][float(buy_order['price'])]['timestamp'] = buy_order['timestamp']
            for sell_order in sell_orders:
                sell_order_dict[pair][float(sell_order['price'])] = {
                    'qty': float(sell_order['amount']),
                    'order_id': sell_order['id'],
                }
                if with_ts:
                    sell_order_dict[pair][float(sell_order['price'])]['timestamp'] = sell_order['timestamp']
            

        for pair in self.pairs:
            open_orders = self.api.get_private_order_list(pair=pair)
            closed_orders = self.api.get_private_closed_order_list(pair=pair)
            if len(open_orders) != 0:
                gather_orders(open_orders, self.open_buy_orders, self.open_sell_orders)
            
            if len(closed_orders) != 0:
                gather_orders(closed_orders, self.closed_buy_orders, self.closed_sell_orders)

    def reset_coin(self, pair):
        """
        Function for reset all the orders of certain coin pair
        """
        self.open_buy_orders[pair] = {}
        self.open_sell_orders[pair] = {}
        self.closed_buy_orders[pair] = {}
        self.closed_sell_orders[pair] = {}
    def update(self, msg):
        """
        Function for updating current open / closed orders, only spot orders are needed to record
        NOTE: 
        The order record update is quite tricky, since some order might only be filled a little bit
        For example, I put a buy order of BTC at 30000, with size 0.2, but only got fill 0.1, which means
        I have btc order of size 0.1 and price 30000 is opened on the market book.
        Which means the open order of btc at price 30000 should be modifiy to 0.1 
        and closed buy order of btc at price 30000 should add 0.1.

        And if the order has been filled / cancel, then developer need to remove the order record from open orders
        and add order record in closed orders

        So basiclly, I used price as dictionary key to track if there are same price order left or open
        Also please remember to record closed timestamp of orders

        inp_args:
            msg : websocket message from exchange
        """
        if msg:
            data = msg['data']
            for single_data in data:
                if single_data['category'].lower() != self.accountType:
                    continue
                pair = single_data['symbol']
                
                #New order, simply add or replace the information of open order
                if single_data['orderStatus'] == 'New':
                    if single_data['side'].upper() == 'BUY':
                        self.open_buy_orders[pair][float(
                            single_data['price'])] = {
                                "qty": float(single_data['qty']),
                                "order_id": single_data['orderId']
                            }
                    else:
                        self.open_sell_orders[pair][float(
                            single_data['price'])] = {
                                "qty": float(single_data['qty']),
                                "order_id": single_data['orderId']
                            }

                #Fill / cancel order, need to remove record from open order and add new record in closed order
                elif single_data['orderStatus'] == 'Filled' or single_data[
                        'orderStatus'] == 'Cancelled' or single_data[
                            'orderStatus'] == 'PartiallyFilledCanceled':
                    if single_data['side'].upper() == 'BUY':
                        del self.open_buy_orders[pair][float(
                            single_data['price'])]
                        if single_data['orderStatus'] == 'Filled':
                            self.closed_buy_orders[pair][float(single_data['price'])] = {
                                "qty" : float(single_data['qty']),
                                "order_id" : single_data['orderId'],
                                'timestamp' : float(single_data['updatedTime'])/1e3
                            }
                            
                    else:
                        del self.open_sell_orders[pair][float(
                            single_data['price'])]
                        if single_data['orderStatus'] == 'Filled':
                            self.closed_sell_orders[pair][float(single_data['price'])] = {
                                "qty" : float(single_data['qty']),
                                "order_id" : single_data['orderId'],
                                'timestamp' : float(single_data['updatedTime'])/1e3
                            }
                #partilly filled, need to modify the open order and add close order
                elif single_data['orderStatus'] == 'PartiallyFilled':
                    if single_data['side'].upper() == 'BUY':
                        self.open_buy_orders[pair][float(
                            single_data['price'])]['qty'] -= float(
                                single_data['qty'])
                        self.closed_buy_orders[pair][float(single_data['price'])] = {
                                "qty" : float(single_data['qty']),
                                "order_id" : single_data['orderId'],
                                'timestamp' : float(single_data['updatedTime'])/1e3
                            }
                    else:
                        self.open_sell_orders[pair][float(
                            single_data['price'])]['qty'] -= float(
                                single_data['qty'])
                        self.closed_sell_orders[pair][float(single_data['price'])] = {
                                "qty" : float(single_data['qty']),
                                "order_id" : single_data['orderId'],
                                'timestamp' : float(single_data['updatedTime'])/1e3
                            }

    def get_buy_orders(self, pair):
        """
        Function for getting open buy orders of certain coin pair
        inp_args:
            pair : coin pair of certain coin pair
        out_args:
            open buy orders of certain coin pair
        """
        return self.open_buy_orders[pair]
    def get_sell_orders(self, pair):
        """
        Function for getting open sell orders of certain coin pair
        inp_args:
            pair : coin pair of certain coin pair
        out_args:
            open sell orders of certain coin pair
        """
        return self.open_sell_orders[pair]
    def get_closed_buy_orders(self, pair):
        """
        Function for getting closed buy orders of certain coin pair
        inp_args:
            pair : coin pair of certain coin pair
        out_args:
            closed buy orders of certain coin pair
        """
        return self.closed_buy_orders[pair]
    def get_closed_sell_orders(self, pair):
        """
        Function for getting closed sell orders of certain coin pair
        inp_args:
            pair : coin pair of certain coin pair
        out_args:
            closed sell orders of certain coin pair
        """
        return self.closed_sell_orders[pair]

    def display(self):
        """
        Function for debug
        """
        print(f"{self.accountType} order info:")
        print(f'Open buy orders : {self.open_buy_orders}')
        print(f'Open sell orders : {self.open_sell_orders}')

#MustDO
def handle_messages(message):

    if 'topic' in message:
        topic = message['topic']
        if topic.startswith("orderbook"):
            try:
                orderbook_handler.update(message)
            except Exception as e:
                print(f"{topic} update failed due to {e}")
            #orderbook_handler.display()
        elif topic.startswith("publicTrade"):
            try:
                trade_handler.update(message)
            except Exception as e:
                print(f"{topic} update failed due to {e}")
            #trade_handler.display()
        elif topic.startswith("wallet"):
            try:
                wallet_handler.update(message)
            except Exception as e:
                print(f"{topic} update failed due to {e}")
            #wallet_handler.display()
        elif topic.startswith("order"):
            try:
                order_handler.update(message)
            except Exception as e:
                print(f"{topic} update failed due to {e}")
            #order_handler.display()



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--pair_config_path', required=True)
    args = parser.parse_args()

    #coin pair trading configs
    trading_configs = []
    with open(args.pair_config_path, "r") as stream:
        data = yaml.load_all(stream, Loader=yaml.FullLoader)
        pair_configs = next(data)
        for pair_config in pair_configs["TradePairs"]:
            trading_configs.append(pair_config)

    #The coin pair names are recorded in config like btc_usdt, eth_usdt...
    coins = [
        trading_config["pair"].replace('_', '').upper()
        for trading_config in trading_configs
    ]

    #private config from certain exchange
    privateConfig = bybitConfig()

    api_key = privateConfig.api_key
    api_secret = privateConfig.api_secret
    api = RestfulAPI(api_key, api_secret)


    #websocket message handler
    orderbook_handler = OrderBookHandler(api=api, pairs=coins, depth=5)
    trade_handler = TradeHandler(pairs=coins, max_record_amount=20)
    order_handler = OrderHandler(pairs=coins, api=api)
    wallet_handler = WalletHander(api=api)

    #trading strategies of each coin pair
    strategies = [
        STRATEGY(api=api,
                 book_handler=orderbook_handler,
                 trade_handler=trade_handler,
                 order_handler=order_handler,
                 wallet_handler=wallet_handler,
                 config=config) for config in trading_configs
    ]

    #MustDO
    #public websocket initialization
    ws_public = WebSocket(
        testnet=False,
        channel_type="spot",
    )

    #MustDO
    #private websocket initialization
    ws_private = WebSocket(
        testnet=False,
        channel_type="private",
        api_key=api_key,
        api_secret=api_secret,
        trace_logging=False,
    )

    #MustDO
    #order book and trade information subscription 
    for coin in coins:
        ws_public.orderbook_stream(50, coin, handle_messages)
        ws_public.trade_stream(coin, handle_messages)
    
    #MustDO
    #wallet and order information subscription
    ws_private.wallet_stream(handle_messages)
    ws_private.order_stream(handle_messages)
    
    
    
    
    async def all_go():
        while True:
            for strategy in strategies:
                await strategy.go()

    asyncio.get_event_loop().run_until_complete(all_go())