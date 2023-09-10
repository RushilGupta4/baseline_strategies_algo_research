from datetime import timedelta
from statistics import mean
import utils
import pandas as pd
import os

from strategies.last_n_average import LastNAverage
from constants import TransactionSide

N_DAYS = 3
TIME_SPAN = 30
BASE_PATH = os.path.join("data", "zerodha_d1")
OUTPUT_BASE_PATH = os.path.join("output", f"last_{N_DAYS}_days_average")


def run_for_symbol(file_name):
    file_path = os.path.join(BASE_PATH, file_name)
    symbol_name = file_name.split(".")[0]

    data = pd.read_csv(file_path)

    data["date"] = pd.to_datetime(data["date"])

    latest_date = data.iloc[-1]["date"]
    oldest_date = latest_date - timedelta(days=TIME_SPAN)

    data = data[data["date"] >= oldest_date]

    data.reset_index(drop=True, inplace=True)

    strategy = LastNAverage(symbol=symbol_name, n_days=N_DAYS, data=data)

    transactions = strategy.run()

    output_path = os.path.join(OUTPUT_BASE_PATH, f"{symbol_name}.xlsx")

    summary_dict = pd.DataFrame(columns=["trades_taken", "returns", "stock_growth"])

    market_start_price = data.iloc[0]["open"]
    market_end_price = data.iloc[-1]["close"]

    returns = []

    for i, row in transactions.iterrows():
        price = row["price"]
        if row["side"].value == TransactionSide.BUY.value:
            returns.append(price)
        else:
            last_return = returns[-1]
            returns[-1] = (price - last_return) / last_return

    trades_taken = len(transactions)
    if trades_taken > 0:
        returns = sum(returns) * 100
    else:
        returns = 0

    stock_growth = (100 * (market_end_price - market_start_price)) / market_start_price

    summary_dict = {
        "returns": round(returns, 10),
        "stock_growth": round(stock_growth, 10),
        "trades_taken": trades_taken,
    }

    summary = pd.DataFrame.from_dict([summary_dict])

    try:
        transactions["side"] = transactions["side"].apply(lambda x: x.value)
        if len(transactions) > 0:
            transactions["timestamp"] = transactions.index
            transactions["timestamp"] = transactions["timestamp"].dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            transactions.reset_index(drop=True, inplace=True)
            transactions.set_index("timestamp", drop=True, inplace=True)
    except Exception as e:
        print(
            f"\nException occured while processing symbol {symbol_name}: {str(e)} * Dataframe: {transactions}\n"
        )

    # ["stock_growth"] = market_end_price /

    with pd.ExcelWriter(output_path) as writer:
        summary.to_excel(writer, sheet_name="Summary", index=False)
        transactions.to_excel(writer, sheet_name="Transactions")

    summary_dict_new = {
        "returns": str(round(returns, 2)).ljust(5),
        "stock_growth": str(round(stock_growth, 2)).ljust(5),
        "trades_taken": str(trades_taken).ljust(2),
    }

    res = [f"{key}: {value.ljust(7)}" for key, value in summary_dict_new.items()]

    print(f"{symbol_name.ljust(15, ' ')} | {' | '.join(res)}")

    return {"symbol": symbol_name, **summary_dict}


def run():
    if not os.path.exists(OUTPUT_BASE_PATH):
        os.makedirs(OUTPUT_BASE_PATH, exist_ok=True)

    files = utils.get_files_in_dir(BASE_PATH)

    # Using utils to be able to switch to different implementations as needed
    executor = utils.get_executor()

    futures = []

    for file in files:
        futures.append(executor.submit(run_for_symbol, file))

    results = []
    for future in futures:
        try:
            res = future.result()
            results.append(res)
        except Exception as e:
            print(f"\nException occured while processing: {str(e)}\n")

    sorted_results = sorted(
        filter(lambda y: y["trades_taken"] > 0, results),
        key=lambda x: x["returns"],
        reverse=True,
    )

    top_5 = sorted_results[:5]
    top_5 = pd.DataFrame.from_dict(top_5)
    bottom_5 = sorted_results[-5:]
    bottom_5 = pd.DataFrame.from_dict(bottom_5)

    average_returns = mean([x["returns"] for x in results])
    average_stock_growth = mean([x["stock_growth"] for x in results])

    summary = {
        "average_returns": average_returns,
        "average_stock_growth": average_stock_growth,
    }

    print("-" * 85)

    print(
        f"SUMMARY (Last {N_DAYS} days profitable) | Average Returns: {round(average_returns, 2)}% | Average Stock Growth: {round(average_stock_growth, 2)}%"
    )

    summary = pd.DataFrame.from_dict([summary])

    file_path = os.path.join(OUTPUT_BASE_PATH, "_summary.xlsx")

    with pd.ExcelWriter(file_path) as writer:
        top_5.to_excel(
            writer, sheet_name="Summary", startcol=0, startrow=0, index=False
        )
        bottom_5.to_excel(
            writer,
            sheet_name="Summary",
            startcol=0,
            startrow=7,
            header=False,
            index=False,
        )
        summary.to_excel(
            writer, sheet_name="Summary", startcol=0, startrow=13, index=False
        )


if __name__ == "__main__":
    run()
