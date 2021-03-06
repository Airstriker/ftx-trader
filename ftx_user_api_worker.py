import os
import sys
import asyncio
import logging
import multiprocessing.queues
from decimal import *
from event_dispatcher import EventDispatcher
from ftx_client import FtxClient
from ftx_lib import FtxApiClient
from queue import Empty
from periodic import PeriodicNormal
from pid import PidFile
from pushover_notifier import PushoverNotifier


class FtxUserApiWorker(object):

    def __init__(self, ftx_client: FtxClient, shared_user_api_data: dict, shared_market_data: dict, buy_sell_requests_queue: multiprocessing.queues.Queue, debug: bool = True, log_file: str = None, transactions_log_file: str = None, pushover_notifier: PushoverNotifier = None):
        print("Initializing ftx user api worker for user: {}".format(ftx_client.ftx_user))
        self.debug = debug
        self.log_file = log_file if log_file else "./logs/ftx_user_api_worker_{}.log".format(ftx_client.ftx_user)
        self.logger = logging.getLogger("ftx_user_api_worker_{}".format(ftx_client.ftx_user))
        FtxUserApiWorker.setup_logger(self.logger, self.log_file)
        self.transactions_log_file = transactions_log_file if transactions_log_file else "./logs/transactions_{}.log".format(ftx_client.ftx_user)
        self.transactions_logger = logging.getLogger("transactions_logger_{}".format(ftx_client.ftx_user))
        FtxUserApiWorker.setup_logger(self.transactions_logger, self.transactions_log_file, mode="a")
        self.ftx_client = ftx_client
        self.shared_market_data = shared_market_data
        self.shared_user_api_data = shared_user_api_data
        self.buy_sell_requests_queue = buy_sell_requests_queue
        self.ftx_api_client = None
        self.initializing = False
        self.initial_requests_list = []
        self.initialized = False
        self.periodic_calls = []
        self.pushover_notifier = pushover_notifier
        self.client_orders = {}

    @staticmethod
    def setup_logger(logger, log_file, mode="w"):
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(log_file, mode=mode)
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(console_formatter)
        fh.setFormatter(file_formatter)
        logger.addHandler(ch)
        logger.addHandler(fh)

    def pushover_notify(self, message, priority=2):
        if self.pushover_notifier:
            message = self.logger.name + ": " + message
            self.pushover_notifier.notify(message, priority)

    def get_instruments(self):
        '''
        Provides information on all supported instruments (e.g. BTC_USDT)
        '''
        self.ftx_api_client.send(
            request={
                "op": "public/get-instruments"
            }
        )

    def handle_response_get_instruments(self, response: dict):
        '''
        Update the decimals for each used ticker
        '''
        self.logger.info("Received response for public/get-instruments method with id: {}".format(response["id"]))
        for ticker, decimals in self.shared_user_api_data["tickers"].items():
            try:
                for instrument in response["result"]["instruments"]:
                    if instrument["instrument_name"] == ticker and instrument.keys() >= decimals.keys():
                        for decimal in decimals.keys():
                            # update the decimal values for the ticker - NOTE! the shared data is updated this way on purpose!
                            # Note! Modifications to mutable values or items in dict and list proxies will not be propagated through the manager, because the proxy has no way of knowing when its values or items are modified. To modify such an item, you can re-assign the modified object to the container proxy
                            copied_dict = self.shared_user_api_data["tickers"]
                            copied_dict[ticker][decimal] = str(instrument[decimal])
                            self.shared_user_api_data["tickers"] = copied_dict

            except Exception as e:
                raise Exception("Cannot get ticker decimals for ticker: {}. Exception: {}".format(ticker, repr(e)))

            # Check if the decimals have been updated for the ticker
            for decimal, value in self.shared_user_api_data["tickers"].items():
                if value == 0:
                    raise Exception("The decimals for ticker: {} have not been updated.".format(ticker))

    def get_user_balances(self):
        '''
        Returns the account balance of a user for all tokens/coins.
        To be triggered only after websockets disconnection.
        '''
        self.ftx_api_client.send(
            request={
                "op": "private/get-account-summary"
            }
        )

    def handle_response_get_user_balances(self, response: dict):
        '''
        "result": {
            "accounts": [
                {
                    "balance": 99999999.905000000000000000,
                    "available": 99999996.905000000000000000,
                    "order": 3.000000000000000000,
                    "stake": 0,
                    "currency": "CRO"
                }
            ]
        }
        '''
        try:
            self.logger.info("Received response for private/get-account-summary method with id: {}".format(response["id"]))
            for balance in response["result"]["accounts"]:
                if balance["currency"] == "USDT":
                    self.logger.info("Updated USDT balance.")
                    self.shared_user_api_data["balance_USDT"] = str(balance["available"])
                elif balance["currency"] == "BTC":
                    self.logger.info("Updated BTC balance.")
                    self.shared_user_api_data["balance_BTC"] = str(balance["available"])
                elif balance["currency"] == "FTT":
                    self.logger.info("Updated FTT balance.")
                    self.shared_user_api_data["balance_FTT"] = str(balance["available"])
        except Exception as e:
            raise Exception("Wrong data structure in private/get-account-summary response: {}. Exception: {}".format(response, repr(e)))

    def create_market_buy_order(self, instrument_name, amount_to_spend):
        '''
        Creates a new BUY order on the Exchange.
        This call is asynchronous, so the response is simply a confirmation of the request with assigned order_id for the given client_oid.
        The user.order subscription can be used to check when the order is successfully created.
        '''
        client_order_id = self.ftx_client.ftx_user + "_BUY_" + instrument_name + "_market_order_" + str(self.ftx_api_client.current_id())
        self.client_orders[client_order_id] = {
            "order_id": "",
            "status": "pending"
        }
        self.ftx_api_client.send(
            request={
                "op": "private/create-order",
                "instrument_name": instrument_name,
                "side": "BUY",
                "type": "MARKET",
                "notional": amount_to_spend,
                "client_oid": client_order_id
            }
        )
        return client_order_id

    def buy_BTC_for_USDT_market_order(self, amount_to_spend):
        return self.create_market_buy_order("BTC_USDT", amount_to_spend)

    def create_market_sell_order(self, instrument_name, quantity_to_be_sold):
        '''
        Creates a new SELL order on the Exchange.
        This call is asynchronous, so the response is simply a confirmation of the request with assigned order_id for the given client_oid.
        The user.order subscription can be used to check when the order is successfully created.
        '''
        client_order_id = self.ftx_client.ftx_user + "_SELL_" + instrument_name + "_market_order_" + str(self.ftx_api_client.current_id())
        self.client_orders[client_order_id] = {
            "order_id": "",
            "status": "pending"
        }
        self.ftx_api_client.send(
            request={
                "op": "private/create-order",
                "instrument_name": instrument_name,
                "side": "SELL",
                "type": "MARKET",
                "quantity": quantity_to_be_sold,
                "client_oid": client_order_id
            }
        )
        return client_order_id

    def sell_BTC_to_USDT_market_order(self, quantity_to_be_sold):
        return self.create_market_sell_order("BTC_USDT", quantity_to_be_sold)

    def handle_response_create_order(self, response: dict):
        '''
        "result": {
            "order_id": "337843775021233500",
            "client_oid": "my_order_0002"
        }
        '''
        try:
            self.logger.info("Received response for private/create-order method with id: {}. Result: {}".format(response["id"], response["result"]))
            client_order_id = response["result"]["client_oid"]
            self.client_orders[client_order_id]["order_id"] = response["result"]["order_id"]
        except Exception as e:
            raise Exception("Wrong data structure in private/create-order response: {}. Exception: {}".format(response, repr(e)))

    def handle_channel_event_user_order(self, event: dict):  #TODO
        '''
        "data": [
            {
                "status": "ACTIVE",
                "side": "BUY",
                "price": 1,
                "quantity": 1,
                "order_id": "366455245775097673",
                "client_oid": "my_order_0002",
                "create_time": 1588758017375,
                "update_time": 1588758017411,
                "type": "LIMIT",
                "instrument_name": "ETH_CRO",
                "cumulative_quantity": 0,
                "cumulative_value": 0,
                "avg_price": 0,
                "fee_currency": "CRO",
                "time_in_force":"GOOD_TILL_CANCEL"
            }
        ]
        '''
        try:
            self.logger.info("Received user orders update. Event: {}".format(event["data"]))
            for order in event["data"]:
                if order["client_oid"] in self.client_orders:
                    pass  #TODO
        except Exception as e:
            raise Exception("Wrong data structure in user.order channel event. Exception: {}".format(repr(e)))

    def handle_channel_event_user_balance(self, event: dict):
        '''
        "data": [
            {
                "currency": "CRO",
                "balance": 99999999947.99626,
                "available": 99999988201.50826,
                "order": 11746.488,
                "stake": 0
            }
        ]
        '''
        try:
            self.logger.info("Received balance update. Event: {}".format(event["data"]))
            for balance in event["data"]:
                if balance["currency"] == "USDT":
                    self.logger.info("Updated USDT balance.")
                    self.shared_user_api_data["balance_USDT"] = str(balance["available"])
                elif balance["currency"] == "BTC":
                    self.logger.info("Updated BTC balance.")
                    self.shared_user_api_data["balance_BTC"] = str(balance["available"])
                elif balance["currency"] == "FTT":
                    self.logger.info("Updated FTT balance.")
                    self.shared_user_api_data["balance_FTT"] = str(balance["available"])
        except Exception as e:
            raise Exception("Wrong data structure in user.balance channel event. Exception: {}".format(repr(e)))

    def handle_buy_request(self, request: dict):
        # Compare the price from request with current market price from ftx
        self.transactions_logger.info("")
        price_in_request = str(request["price"])
        self.shared_user_api_data["last_transaction_BTC_buy_price_in_fiat"] = price_in_request
        price_on_ftx = self.shared_market_data["price_BTC_buy_for_USDT"]
        self.shared_user_api_data["last_transaction_BTC_buy_price_in_USDT"] = price_on_ftx
        fiat = request["fiat"]
        eur_usd_exchange_rate = self.shared_market_data["EUR_USD_exchange_rate"]
        if fiat == "EUR" and eur_usd_exchange_rate != 0:
            price_in_request_in_usd = Decimal(price_in_request).quantize(Decimal('1e-' + str(2))) * Decimal(eur_usd_exchange_rate).quantize(Decimal('1e-' + str(2)))
            self.logger.info("[BUY REQUEST] received! Price in request: {} [{}] ({} [USD]). Price on ftx: {} [USDT]".format(Decimal(price_in_request).quantize(Decimal('1e-' + str(2))), fiat, price_in_request_in_usd, Decimal(price_on_ftx).quantize(Decimal('1e-' + str(2)))))
            message = "[BUY] Price in request: {} [{}] ({} [USD]). Price on ftx: {} [USDT]".format(Decimal(price_in_request).quantize(Decimal('1e-' + str(2))), fiat, price_in_request_in_usd.quantize(Decimal('1e-' + str(2))), Decimal(price_on_ftx).quantize(Decimal('1e-' + str(2))))
            self.transactions_logger.info(message)
            self.pushover_notify(message)
        else:
            self.logger.info("[BUY REQUEST] received! Price in request: {} [{}]. Price on ftx: {} [USDT]".format(Decimal(price_in_request).quantize(Decimal('1e-' + str(2))), fiat, Decimal(price_on_ftx).quantize(Decimal('1e-' + str(2)))))
            message = "[BUY] Price in request: {} [{}]. Price on ftx: {} [USDT]".format(Decimal(price_in_request).quantize(Decimal('1e-' + str(2))), fiat, Decimal(price_on_ftx).quantize(Decimal('1e-' + str(2))))
            self.transactions_logger.info(message)
            self.pushover_notify(message)

        # Get real :)
        price_BTC_buy_for_USDT = Decimal(self.shared_market_data["price_BTC_buy_for_USDT"]).quantize(
            Decimal('1e-' + str(self.shared_user_api_data["tickers"]["BTC_USDT"]["price_decimals"])), rounding=ROUND_UP)
        self.transactions_logger.debug("price_BTC_buy_for_USDT (Decimal): {}".format(price_BTC_buy_for_USDT))
        balance_USDT = Decimal(self.shared_user_api_data["balance_USDT"]).quantize(Decimal('1e-' + str(2)), rounding=ROUND_DOWN)
        self.transactions_logger.debug("balance_USDT (Decimal): {}".format(balance_USDT))
        taker_fee = Decimal(self.shared_market_data["taker_fee"]).quantize(Decimal('1e-' + str(4)), rounding=ROUND_UP)
        self.transactions_logger.debug("taker_fee (Decimal): {}".format(taker_fee))
        fee_BTC_buy_in_BTC = ((balance_USDT / price_BTC_buy_for_USDT) * taker_fee).quantize(Decimal('1e-' + str(8)), rounding=ROUND_UP)
        self.transactions_logger.debug("fee_BTC_buy_in_BTC (Decimal): {}".format(fee_BTC_buy_in_BTC))
        balance_FTT = Decimal(self.shared_user_api_data["balance_FTT"]).quantize(Decimal('1e-' + str(8)), rounding=ROUND_DOWN)
        self.transactions_logger.debug("balance_FTT (Decimal): {}".format(balance_FTT))

        message = "Placing a market order on BTC/USDT pair for USDT balance: {}".format(balance_USDT)
        self.logger.info(message)
        #self.pushover_notify(message)

    def handle_sell_request(self, request: dict):
        # Compare the price from request with current market price from ftx
        self.transactions_logger.info("")
        price_in_request = str(request["price"])
        self.shared_user_api_data["last_transaction_BTC_sell_price_in_fiat"] = price_in_request
        price_on_ftx = self.shared_market_data["price_BTC_sell_to_USDT"]
        self.shared_user_api_data["last_transaction_BTC_sell_price_in_USDT"] = price_on_ftx
        fiat = request["fiat"]
        profit_in_fiat = (Decimal(self.shared_user_api_data["last_transaction_BTC_sell_price_in_fiat"]).quantize(Decimal('1e-' + str(2))) - Decimal(self.shared_user_api_data["last_transaction_BTC_buy_price_in_fiat"]).quantize(Decimal('1e-' + str(2)))) if self.shared_user_api_data["last_transaction_BTC_buy_price_in_fiat"] else Decimal('0').quantize(Decimal('1e-' + str(2)))
        profit_in_usdt = (Decimal(self.shared_user_api_data["last_transaction_BTC_sell_price_in_USDT"]).quantize(Decimal('1e-' + str(2))) - Decimal(self.shared_user_api_data["last_transaction_BTC_buy_price_in_USDT"]).quantize(Decimal('1e-' + str(2)))) if self.shared_user_api_data["last_transaction_BTC_buy_price_in_USDT"] else Decimal('0').quantize(Decimal('1e-' + str(2)))
        eur_usd_exchange_rate = self.shared_market_data["EUR_USD_exchange_rate"]
        if fiat == "EUR" and eur_usd_exchange_rate != 0:
            price_in_request_in_usd = Decimal(price_in_request).quantize(Decimal('1e-' + str(2))) * Decimal(eur_usd_exchange_rate).quantize(Decimal('1e-' + str(2)))
            profit_in_fiat_in_usd = Decimal(profit_in_fiat).quantize(Decimal('1e-' + str(2))) * Decimal(eur_usd_exchange_rate).quantize(Decimal('1e-' + str(2)))
            self.logger.info("[SELL REQUEST] received! Price in request: {} [{}] ({} [USD]). Price on ftx: {} [USDT]. Profit in fiat: {} [{}] ({} [USD]). Profit on ftx: {} [USDT].".format(Decimal(price_in_request).quantize(Decimal('1e-' + str(2))), fiat, price_in_request_in_usd.quantize(Decimal('1e-' + str(2))), Decimal(price_on_ftx).quantize(Decimal('1e-' + str(2))), profit_in_fiat, fiat, profit_in_fiat_in_usd.quantize(Decimal('1e-' + str(2))), profit_in_usdt))
            message = "[SELL] Price in request: {} [{}] ({} [USD]). Price on ftx: {} [USDT]. Profit in fiat: {} [{}] ({} [USD]). Profit on ftx: {} [USDT].".format(Decimal(price_in_request).quantize(Decimal('1e-' + str(2))), fiat, price_in_request_in_usd.quantize(Decimal('1e-' + str(2))), Decimal(price_on_ftx).quantize(Decimal('1e-' + str(2))), profit_in_fiat, fiat, profit_in_fiat_in_usd.quantize(Decimal('1e-' + str(2))), profit_in_usdt)
            self.transactions_logger.info(message)
            self.pushover_notify(message)
        else:
            self.logger.info("[SELL REQUEST] received! Price in request: {} [{}]. Price on ftx: {} [USDT]. Profit in fiat: {} [{}]. Profit on ftx: {} [USDT].".format(Decimal(price_in_request).quantize(Decimal('1e-' + str(2))), fiat, Decimal(price_on_ftx).quantize(Decimal('1e-' + str(2))), profit_in_fiat, fiat, profit_in_usdt))
            message = "[SELL] Price in request: {} [{}]. Price on ftx: {} [USDT]. Profit in fiat: {} [{}]. Profit on ftx: {} [USDT].".format(Decimal(price_in_request).quantize(Decimal('1e-' + str(2))), fiat, Decimal(price_on_ftx).quantize(Decimal('1e-' + str(2))), profit_in_fiat, fiat, profit_in_usdt)
            self.transactions_logger.info(message)
            self.pushover_notify(message)

        # Get real :)
        price_BTC_sell_to_USDT = Decimal(self.shared_market_data["price_BTC_sell_to_USDT"]).quantize(
            Decimal('1e-' + str(self.shared_user_api_data["tickers"]["BTC_USDT"]["price_decimals"])), rounding=ROUND_UP)
        self.transactions_logger.debug("price_BTC_sell_to_USDT (Decimal): {}".format(price_BTC_sell_to_USDT))
        balance_BTC = Decimal(self.shared_user_api_data["balance_BTC"]).quantize(Decimal('1e-' + str(8)), rounding=ROUND_DOWN)
        self.transactions_logger.debug("balance_BTC (Decimal): {}".format(balance_BTC))
        taker_fee = Decimal(self.shared_market_data["taker_fee"]).quantize(Decimal('1e-' + str(4)), rounding=ROUND_UP)
        self.transactions_logger.debug("taker_fee (Decimal): {}".format(taker_fee))
        fee_BTC_sell_in_USDT = ((balance_BTC * price_BTC_sell_to_USDT) * taker_fee).quantize(Decimal('1e-' + str(2)), rounding=ROUND_UP)
        self.transactions_logger.debug("fee_BTC_sell_in_USDT (Decimal): {}".format(fee_BTC_sell_in_USDT))
        balance_FTT = Decimal(self.shared_user_api_data["balance_FTT"]).quantize(Decimal('1e-' + str(8)), rounding=ROUND_DOWN)
        self.transactions_logger.debug("balance_FTT (Decimal): {}".format(balance_FTT))

        message = "Placing a market order on BTC/USDT pair for BTC balance: {}".format(balance_BTC)
        self.logger.info(message)
        #self.pushover_notify(message)

    def handle_buy_sell_requests(self):
        '''
        The incoming request should be a dict with the following keys:
        {
            'price': '25420',
            'type': 'sell'
        }
        '''
        try:
            request = self.buy_sell_requests_queue.get_nowait()
        except (Empty, BrokenPipeError):
            pass
        else:
            if request:
                if "type" in request and "price" in request and "fiat" in request:
                    if request["type"] == "buy":
                        self.handle_buy_request(request)
                    elif request["type"] == "sell":
                        self.handle_sell_request(request)
                    else:
                        raise Exception("Unknown 'type' key value in buy/sell request! Request: {}".format(request))
                else:
                    raise Exception("The incoming buy/sell request doesn't contain required keys! Request: {}".format(request))

    async def run(self):

        '''
        shared_market_data = {
            "taker_fee": exchange_variables["taker_fee"], -> done
            "CRO_holding_backup": exchange_variables["CRO_holding_backup"], -> done
            "balance_USDT": 0,                  -> done
            "balance_BTC": 0,                   -> done
            "balance_CRO": 0,                   -> done
            "price_BTC_sell_to_USDT": 0,        -> done
            "price_CRO_buy_for_BTC": 0,         -> done
            "fee_BTC_sell_in_USDT": 0,
            "price_CRO_buy_for_USDT": 0,        -> done
            "last_CRO_price_in_USDT": 0,        -> done
            "last_CRO_price_in_BTC": 0,         -> done
            "fee_BTC_sell_in_CRO": 0,
            "price_BTC_buy_for_USDT": 0,        -> done
            "fee_BTC_buy_in_BTC": 0,
            "fee_BTC_buy_in_CRO": 0,
        }
        '''

        self.ftx_api_client = FtxApiClient(
            client_type=FtxApiClient.USER,
            debug=self.debug,
            logger=self.logger,
            pushover_notifier=self.pushover_notifier,
            api_key=self.ftx_client.ftx_api_key,
            api_secret=self.ftx_client.ftx_api_secret,
            channels=[
                # "user.balance",
                # "user.order"
            ],
            channels_handling_map={
                # "user.balance": self.handle_channel_event_user_balance,
                # "user.order": self.handle_channel_event_user_order
            },
            responses_handling_map={
                # "public/get-instruments": self.handle_response_get_instruments,
                # "private/get-account-summary": self.handle_response_get_user_balances,
                # "private/create-order": self.handle_response_create_order
            },
            initial_requests_handling_map={
                # "private/get-account-summary": self.get_user_balances,
                # "public/get-instruments": self.get_instruments
            },
            # periodic_requests_handling_map={
            #     "public/get-instruments": self.get_instruments
            # }
        )
        self.pushover_notify("Started!", 1)

        while True:
            await asyncio.sleep(0)  # This line is VERY important: In the case of trying to concurrently run two looping Tasks (here handle_requests() and handle_events_and_responses()), unless the Task has an internal await expression, it will get stuck in the while loop, effectively blocking other tasks from running (much like a normal while loop). However, as soon the Tasks have to (a)wait, they run concurrently without an issue. Check this: https://stackoverflow.com/questions/29269370/how-to-properly-create-and-run-concurrent-tasks-using-pythons-asyncio-module
            # Handle externally injected buy/sell requests
            if self.ftx_api_client.initialized:
                try:
                    self.handle_buy_sell_requests()
                except Exception as e:
                    message = "Exception during handling buy/sell request: {}".format(repr(e))
                    self.logger.exception(message)
                    self.pushover_notify(message)
                    await asyncio.sleep(1)

    async def cleanup(self):
        self.logger.info("Cleanup before closing worker...")

    def run_forever(self):
        # executor = ProcessPoolExecutor(2)  # Alternatively ThreadPoolExecutor
        # boo = asyncio.create_task(loop.run_in_executor(executor, say_boo))  # say_boo should be ordinary functions
        # baa = asyncio.create_task(loop.run_in_executor(executor, say_baa))
        # loop.run_forever()
        with PidFile(pidname="ftx_user_api_worker", piddir="./logs") as pidfile:
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.run())
            except KeyboardInterrupt:
                self.logger.info("Interrupted")
                asyncio.get_event_loop().run_until_complete(self.cleanup())
                pidfile.close(fh=pidfile.fh, cleanup=True)
                self.pushover_notify("Interrupted! Bye bye!")
                self.logger.info("Bye bye!")
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
            except Exception as e:
                self.pushover_notify(repr(e))
                self.logger.exception(repr(e))
            finally:
                asyncio.get_event_loop().run_until_complete(self.cleanup())
                pidfile.close(fh=pidfile.fh, cleanup=True)
                self.pushover_notify("Bye bye!")
                self.logger.info("Bye bye!")

