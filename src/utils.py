from datetime import datetime, timedelta
import math
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import traceback
import dotenv
import pandas as pd


dotenv.load_dotenv()


def get_files_in_dir(dir):
    paths = []

    for fname in os.listdir(dir):
        path = os.path.join(dir, fname)
        if os.path.isdir(path):
            # skip directories
            continue

        paths.append(fname)

    return paths


def get_executor(workers=5):
    return ThreadPoolExecutor(max_workers=workers)


def get_data_from_path(file_path, timespan) -> pd.DataFrame:
    data = pd.read_csv(file_path)

    if "time" in data.columns:
        data.rename(inplace=True, columns={"time": "date"})

    data["date"] = pd.to_datetime(data["date"])

    latest_date = data.iloc[-1]["date"]
    latest_date = latest_date.replace(hour=9, minute=15, second=0)
    oldest_date = latest_date - timedelta(days=timespan + 1)

    data = data[data["date"] > oldest_date]

    data.reset_index(drop=True, inplace=True)
    return data


def get_returns_from_transactions(transactions: pd.DataFrame, capital: float) -> float:
    if len(transactions) <= 0:
        return 0

    transactions = transactions.copy(deep=True)
    transactions["true_price"] = transactions["price"] * transactions["quantity"]

    grouped = transactions.groupby("side")
    sides_sum = grouped.sum(numeric_only=True)
    sides_sum = grouped.mean(numeric_only=True)

    try:
        buy_price_sum = sides_sum.loc["BUY"]["true_price"]
        sell_price_sum = sides_sum.loc["SELL"]["true_price"]
    except Exception as e:
        print("Transactions")
        print(transactions)
        print()
        print("sides_sum")
        print(sides_sum)
        raise e

    points_made = sell_price_sum - buy_price_sum

    return (points_made / capital) * 100


def get_formatted_traceback(exc, length=9999):
    try:
        tb_str = traceback.format_exception(
            etype=type(exc), value=exc, tb=exc.__traceback__
        )
        res = ""
        for i in range(min(length, len(tb_str))):
            res += tb_str[i] + " "
        if len(res) == 0:
            res = str(exc)
        return res
    except Exception as e:
        return str(exc)


def get_quantity(price: float, capital: float) -> int:
    return math.floor(capital / price)


def should_save_files() -> bool:
    """
    Checks env for the SAVE_FILE variable. Must be true for files to be saved
    """

    should_save = os.getenv("SAVE_FILE")
    return should_save == "true"
