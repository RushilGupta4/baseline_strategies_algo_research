from run.runner import run_with_dataframe
import utils
import pandas as pd
import os

from strategies.last_n_consecutive import LastNConsecutive

N_DAYS = 5
TIME_SPAN = 30
BASE_PATH = os.path.join("data", "zerodha_d1")
OUTPUT_BASE_PATH = os.path.join("output", f"last_{N_DAYS}_days_consecutive")


def run_for_symbol(file_name):
    file_path = os.path.join(BASE_PATH, file_name)
    symbol_name = file_name.split(".")[0]

    data = utils.get_data_from_path(file_path, timespan=TIME_SPAN)

    strategy = LastNConsecutive(symbol=symbol_name, n_days=N_DAYS, data=data)

    try:
        res = strategy.run()
        transactions: pd.DataFrame = res["transactions"]
        capital = res["capital"]
    except Exception as e:
        print(f"\nException occured while running for symbol {symbol_name}: {str(e)}\n")

    output_path = os.path.join(OUTPUT_BASE_PATH, f"{symbol_name}.xlsx")

    summary_dict = pd.DataFrame(columns=["trades_taken", "returns", "stock_growth"])

    market_start_price = data.iloc[0]["open"]
    market_end_price = data.iloc[-1]["close"]

    returns = utils.get_returns_from_transactions(transactions, capital)
    trades_taken = len(transactions)
    stock_growth = (100 * (market_end_price - market_start_price)) / market_start_price

    summary_dict = {
        "returns": round(returns, 10),
        "stock_growth": round(stock_growth, 10),
        "trades_taken": trades_taken,
    }

    summary = pd.DataFrame.from_dict([summary_dict])

    if utils.should_save_files():
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
    run_with_dataframe(
        name=f"Last {N_DAYS} days consecutive",
        output_dir=OUTPUT_BASE_PATH,
        input_dir=BASE_PATH,
        func=run_for_symbol,
    )


if __name__ == "__main__":
    run()
