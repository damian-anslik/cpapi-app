import time
import logging
import google.cloud.firestore as firestore
from app import models
from cpapi import session


class OrderManager:
    """
    The order manager is responsible for processing orders submitted by the user. There are two types of orders:

    - Market orders: orders that are executed immediately at the current market price.
    - Limit orders: orders that are executed only when the market price reaches a specified price.

    The order manager needs to keep track of all of the symbols. It then gets regular snapshots of the market prices. It uses this information to determine whether or not to execute limit orders.
    """

    def __init__(
        self,
        orders_collection: firestore.CollectionReference,
        portfolios_collection: firestore.CollectionReference,
        contracts_collection: firestore.CollectionReference,
        api_session: session.OAuthSession,
    ):
        self.__orders_collection = orders_collection
        self.__contracts_collection = contracts_collection
        self.__portfolios_collection = portfolios_collection
        self.__api_session = api_session
        self.__orders = []
        self.__symbol_conid_map = {}
        self.__symbol_price_map = {}

    def __update_orders(self):
        """
        Update the list of orders from the orders database.
        """
        orders = self.__orders_collection.get()
        logging.info(f"Num orders to process: {len(orders)}")
        self.__orders = [models.Order.parse_obj(order.to_dict()) for order in orders]
        logging.debug(f"Orders: {self.__orders}")

    def __map_orders_to_conids(self):
        """
        Map the symbols in the orders to their conids so that we can get market data snapshots for them.
        """
        self.__symbol_conid_map = {}
        self.__symbol_price_map = {}
        if len(self.__orders) == 0:
            return
        logging.info("Mapping order symbols to conids")
        contracts = self.__contracts_collection.where(
            "symbol", "in", [order.symbol for order in self.__orders]
        ).get()
        for contract in contracts:
            contract_data = contract.to_dict()
            self.__symbol_conid_map[contract_data.get("symbol")] = contract_data.get(
                "con_id"
            )
        logging.debug(f"Symbol conid map: {self.__symbol_conid_map}")

    def __get_market_data_snapshots(self):
        """
        Get market data snapshots for all of the symbols in the orders.
        """
        FIELDS = ["84", "86"]
        conids = list(self.__symbol_conid_map.values())
        if not conids:
            return
        logging.info(f"Getting market data snapshots for conids: {conids}")
        try:
            snapshots = self.__api_session.market_data_snapshot(conids, FIELDS)
        except Exception as e:
            logging.error(f"Error getting market data snapshots: {e}")
            return None
        for snapshot in snapshots:
            symbol = list(self.__symbol_conid_map.keys())[
                list(self.__symbol_conid_map.values()).index(int(snapshot["conidEx"]))
            ]
            has_fields = all(field in snapshot for field in FIELDS)
            if not has_fields:
                continue
            self.__symbol_price_map[symbol] = {
                "bid": snapshot["84"],
                "ask": snapshot["86"],
            }
        logging.debug(f"Symbol price map: {self.__symbol_price_map}")

    def __process_orders(self):
        """
        Process a list of orders.
        """
        logging.info("Processing orders")
        for order in self.__orders:
            logging.info(f"Processing order: {order.symbol}, symbol_price_map: {self.__symbol_price_map}")
            if order.symbol not in self.__symbol_price_map:
                # Skip orders for which we don't have market data.
                logging.info(
                    "Skipping order because we don't have market data for it."
                )
                continue
            self.__process_order_single(order)

    def __process_order_single(self, order: models.Order):
        """
        Process a single order. A different method is used for each order type.
        """
        ask_price = float(self.__symbol_price_map[order.symbol]["ask"])
        bid_price = float(self.__symbol_price_map[order.symbol]["bid"])
        match order.order_type:
            case "MKT":
                self.__process_market_order(order, ask_price, bid_price)
            case "LMT":
                self.__process_limit_order(order, ask_price, bid_price)
            case _:
                logging.error(f"Invalid order type: {order.order_type}, defaulting to MKT")
                self.__process_market_order(order, ask_price, bid_price)

    def __create_position(
        self, order: models.Order, fill_price: float
    ) -> models.Position:
        """
        Create a new position and update the user's portfolio.
        """
        logging.debug(f"Creating position for order: {order}")
        position = models.Position(
            symbol=order.symbol,
            quantity=order.quantity,
            side=order.side,
            conid=self.__symbol_conid_map[order.symbol],
            value=order.quantity * fill_price
        )
        logging.debug(f"New position details: {position}")
        return position

    def __process_market_order(
        self, order: models.Order, ask_price: float, bid_price: float
    ):
        """
        Process a market order.
        """
        logging.debug(f"Processing market order: {order}")
        fill_price = ask_price if order.side == "BUY" else bid_price
        position = self.__create_position(order, fill_price)
        self.__update_positions(position, order)

    def __process_limit_order(
        self, order: models.Order, ask_price: float, bid_price: float
    ):
        """
        Process a limit order.
        """
        logging.debug(f"Processing limit order: {order}")
        order_limit_price = order.limit_price
        if order.side == "BUY" and ask_price > order_limit_price:
            return
        if order.side == "SELL" and bid_price < order_limit_price:
            return
        fill_price = ask_price if order.side == "BUY" else bid_price
        position = self.__create_position(order, fill_price)
        self.__update_positions(position, order)

    def __update_positions(self, new_position: models.Position, order: models.Order):
        """
        Update the positions database with the new position and remove the order from the orders database.
        If the user already has a position in the same symbol, then the position is updated.
        If the order side is SELL, then the position is reduced, otherwise it is increased.
        """
        logging.debug(f"Updating positions for order: {order}")
        portfolio = self.__portfolios_collection.document(order.portfolio_id).get()
        portfolio_data = portfolio.to_dict()
        logging.info(f"New position: {new_position}")
        logging.debug(f"Portfolio data: {portfolio_data}")
        portfolio = models.Portfolio.parse_obj(portfolio_data)
        existing_position = next(
            (
                position
                for position in portfolio.positions
                if position.symbol == new_position.symbol
            ),
            None,
        )
        logging.info(f"Existing position: {existing_position}")
        if existing_position and existing_position.side != None:
            updated_position = self.__update_existing_position(
                existing_position, new_position
            )
            portfolio.positions = [
                updated_position.dict()
                if position.symbol == updated_position.symbol
                else position
                for position in portfolio.positions
            ]
        else:
            logging.debug("Creating new position.")
            portfolio = self.__add_new_position(new_position, portfolio)
        portfolio = self.__remove_order(order, portfolio)
        self.__portfolios_collection.document(order.portfolio_id).set(portfolio.dict())

    def __update_existing_position(
        self, existing_position: models.Position, new_position: models.Position
    ):
        """
        Update an existing position with a new position.
        """
        if existing_position.side == "BUY":
            updated_position = self.__update_existing_buy_position(existing_position, new_position)
        elif existing_position.side == "SELL":
            updated_position = self.__update_existing_sell_position(existing_position, new_position)
        if updated_position.quantity < 0:
            updated_position.side = "SELL"
        elif updated_position.quantity > 0:
            updated_position.side = "BUY"
        else:
            updated_position.side = None
        updated_position.quantity = updated_position.quantity
        updated_position.value = updated_position.value
        return updated_position

    def __update_existing_buy_position(self, existing_position: models.Position, new_position: models.Position) -> models.Position:
        """
        Update an existing BUY position with a new position.
        """
        # If new position is a BUY, then increase the quantity and value of the existing position.
        # If new position is a SELL, then reduce the quantity and value of the existing position.
        if new_position.side == "BUY":
            existing_position.quantity += new_position.quantity
            existing_position.value += new_position.value
        else:
            existing_position.quantity -= new_position.quantity
            existing_position.value -= new_position.value
        return existing_position

    def __update_existing_sell_position(self, existing_position: models.Position, new_position: models.Position) -> models.Position:
        """
        Update an existing SELL position with a new position.
        """
        # If new position is a BUY, then reduce the quantity and value of the existing position.
        # If new position is a SELL, then increase the quantity and value of the existing position.
        if new_position.side == "BUY":
            existing_position.quantity -= new_position.quantity
            existing_position.value -= new_position.value
        else:
            existing_position.quantity += new_position.quantity
            existing_position.value += new_position.value
        return existing_position

    def __add_new_position(
        self, new_position: models.Position, portfolio: models.Portfolio
    ) -> dict:
        portfolio.positions.append(new_position.dict())
        return portfolio

    def __remove_order(
        self, order: models.Order, portfolio: models.Portfolio
    ) -> models.Portfolio:
        """
        Remove an order from the orders database and the user's portfolio.
        """
        self.__orders_collection.document(order.id).delete()
        portfolio.orders = list(filter(lambda o: o.id != order.id, portfolio.orders))
        return portfolio

    def __cancel_market_data_subs(self):
        """
        Cancel existing market data subscriptions.
        """
        logging.info("Cancelling existing market data subscriptions")
        self.__api_session.cancel_market_data_all()

    def run(self):
        """
        Run the order manager loop.
        """
        NUM_REQUESTS_BEFORE_CANCELLING_EXISTING_SUBS = 10
        TIMEOUT_BETWEEN_REQUESTS = 5
        num_requests = 0
        try:
            while True:
                self.__update_orders()
                self.__map_orders_to_conids()
                self.__get_market_data_snapshots()
                self.__process_orders()
                num_requests += 1
                if num_requests == NUM_REQUESTS_BEFORE_CANCELLING_EXISTING_SUBS:
                    self.__cancel_market_data_subs()
                    num_requests = 0
                time.sleep(TIMEOUT_BETWEEN_REQUESTS)
        except KeyboardInterrupt:
            self.__cancel_market_data_subs()
            logging.info("Exiting...")
