import fastapi
import threading
import datetime
import dotenv
import logging
import numpy
import uvicorn
import google.cloud.firestore as firestore
from app.manager import OrderManager
from app.utils import init_api_session, keep_api_session_alive

app = fastapi.FastAPI()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
dotenv.load_dotenv()
firestore_client = firestore.Client()
orders_collection = firestore_client.collection("orders")
users_collection = firestore_client.collection("users")
portfolios_collection = firestore_client.collection("portfolios")
contracts_collection = firestore_client.collection("symbols")
hmds_collection = firestore_client.collection("hmds")
api_session = init_api_session()


def filter_bars(bars: list[dict]) -> list[dict]:
    """
    Filter out bars that are above a moving average threshold.
    """
    MOVING_AVERAGE_WINDOW = 3
    MOVING_AVERAGE_FILTER_THRESHOLD = 1.5
    open_prices = [bar["o"] for bar in bars]
    open_price_ma = (
        numpy.convolve(open_prices, numpy.ones(MOVING_AVERAGE_WINDOW), "valid")
        / MOVING_AVERAGE_WINDOW
    )
    open_prices_ma = numpy.concatenate(
        (numpy.full(10, numpy.nan), open_price_ma), axis=None
    )
    filtered_bars = [
        bar
        for bar, open_ma in zip(bars, open_prices_ma)
        if bar["o"] < MOVING_AVERAGE_FILTER_THRESHOLD * open_ma
    ]
    return filtered_bars


def request_historical_data(
    conid: str, period: str, bar: str, start: datetime = None
) -> list[dict]:
    """
    Retrieve historical data for a contract.
    """
    logging.info(f"Requesting historical data for {conid}; {type(conid)}")
    response = api_session.historical_market_data(
        conid=str(conid),
        period=period,
        bar=bar,
        start_time=start,
        outside_rth=True,
    )
    bars = response["data"]
    filtered_bars = filter_bars(bars)
    return filtered_bars


@app.get("/hmds")
async def get_historical_market_data(
    symbol: str,
):
    """
    Get historical market data for a symbol. If the symbol is not found, then raise an exception.
    If the symbol is found in the database, then return the historical market data from the database,
    otherwise, retrieve the historical market data from the API and store it in the database.
    """
    symbol_upper = symbol.upper()
    market_data_doc = hmds_collection.document(symbol_upper).get()
    market_data = market_data_doc.to_dict()
    if market_data:
        market_data_last_updated = datetime.datetime.fromisoformat(
            market_data["last_updated"]
        )
        is_stale_data = (
            datetime.datetime.now() - market_data_last_updated
            > datetime.timedelta(days=1)
        )
        if not is_stale_data:
            return market_data
    contract = contracts_collection.where("symbol", "==", symbol_upper).get()[0]
    if not contract:
        raise fastapi.HTTPException(status_code=404, detail="Symbol not found")
    contract_data = contract.to_dict()
    conid = contract_data["con_id"]
    historical_data = request_historical_data(conid, "1y", "1d")
    market_data = {
        "symbol": symbol.upper(),
        "last_updated": datetime.datetime.now().isoformat(),
        "bars": historical_data,
    }
    hmds_collection.document(symbol_upper).set(market_data)
    return market_data


@app.get("/snapshot")
async def get_market_data_snapshots(conids: str) -> list[dict]:
    conid_list = conids.split(",")
    fields = ["84", "86"]
    try:
        snapshot_response = api_session.market_data_snapshot(conid_list, fields)
    except Exception as e:
        logging.error(e)
        raise fastapi.HTTPException(status_code=500, detail=str(e))
    prices = []
    for snapshot in snapshot_response:
        # Check if snapshot has ask and bid prices
        if "86" not in snapshot or "84" not in snapshot:
            continue
        ask_price = float(snapshot["86"])
        bid_price = float(snapshot["84"])
        prices.append(
            {
                "conid": snapshot["conid"],
                "ask_price": ask_price,
                "bid_price": bid_price,
            }
        )
    return prices


if __name__ == "__main__":
    session_management_thread = threading.Thread(
        target=keep_api_session_alive, args=(api_session,)
    )
    order_manager = OrderManager(
        orders_collection, portfolios_collection, contracts_collection, api_session
    )
    order_manager_thread = threading.Thread(target=order_manager.run)
    session_management_thread.start()
    order_manager_thread.start()
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)