from datetime import timedelta
from statistics import mean
from run.runner import run_with_dataframe
import utils
import pandas as pd
import os
from warnings import simplefilter

from strategies.market_open_option_buying import MarketOpenOptionBuying
from constants import TransactionSide

TIME_SPAN = 30
BASE_PATH = os.path.join("data", "zerodha_m5_options")
OUTPUT_BASE_PATH = os.path.join("output", f"market_buying")

simplefilter("ignore")


def run_for_symbol(dir_path, file_name):
    file_path = os.path.join(dir_path, file_name)

    data = pd.read_csv(file_path)
    data["time"] = pd.to_datetime(data["time"])
    data.reset_index(drop=True, inplace=True)

    symbol_name = file_name.split(".")[0]

    strategy = MarketOpenOptionBuying(symbol=symbol_name, data=data)

    res = strategy.run()
    transactions: pd.DataFrame = res["transactions"]
    capital = res["capital"]

    market_start_price = data.iloc[0]["open"]
    market_end_price = data.iloc[-1]["close"]

    trades_taken = len(transactions)
    returns = utils.get_returns_from_transactions(transactions, capital)
    stock_growth = (100 * (market_end_price - market_start_price)) / market_start_price

    summary_dict = {
        "returns": round(returns, 10),
        "stock_growth": round(stock_growth, 10),
        "trades_taken": trades_taken,
    }

    return {"symbol": symbol_name, "transactions": transactions, **summary_dict}


def get_other_side_file(filename):
    if "CE" in filename:
        return filename.replace("CE", "PE")

    return filename.replace("PE", "CE")


def run_for_date(date):
    dir_path = os.path.join(BASE_PATH, date)
    symbols = utils.get_files_in_dir(dir_path)

    symbols.sort()

    file_one = symbols[len(symbols) // 2]
    file_two = get_other_side_file(file_one)

    summary_dicts = []
    for file_name in [file_one, file_two]:
        summary_dict = run_for_symbol(dir_path, file_name)
        summary_dicts.append(summary_dict)

    transactions = pd.concat([i["transactions"] for i in summary_dicts])

    output_path = os.path.join(OUTPUT_BASE_PATH, f"{date}.xlsx")

    summary_dict = {
        "returns": mean([x["returns"] for x in summary_dicts]),
        "stock_growth": mean([x["stock_growth"] for x in summary_dicts]),
        "trades_taken": sum([x["trades_taken"] for x in summary_dicts]),
    }

    summary = pd.DataFrame.from_dict([summary_dict])

    if utils.should_save_files():
        with pd.ExcelWriter(output_path) as writer:
            summary.to_excel(writer, sheet_name="Summary", index=False)
            transactions.to_excel(writer, sheet_name="Transactions")

    summary_dict_new = {
        "returns": str(round(summary_dict["returns"], 2)).ljust(5),
        "stock_growth": str(round(summary_dict["stock_growth"], 2)).ljust(5),
        "trades_taken": str(summary_dict["trades_taken"]).ljust(2),
    }

    res = [f"{key}: {value.ljust(10)}" for key, value in summary_dict_new.items()]

    print(f"{date.ljust(15, ' ')} | {' | '.join(res)}")
    return {"date": date, **summary_dict}


def run():
    run_with_dataframe(
        name=f"Market Open Option Buying",
        output_dir=OUTPUT_BASE_PATH,
        input_dir=BASE_PATH,
        func=run_for_date,
        include_dirs=True,
        subject="Date",
    )


if __name__ == "__main__":
    run()
