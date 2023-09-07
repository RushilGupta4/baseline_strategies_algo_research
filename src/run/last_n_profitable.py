import utils
import pandas as pd
import os

from strategies.last_n_profitable import LastNProfitable

N_DAYS = 2
BASE_PATH = os.path.join("data", "zerodha_d1")
OUTPUT_BASE_PATH = os.path.join("output", f"last_{N_DAYS}_days_profitable")


def run_for_symbol(file_name):
    file_path = os.path.join(BASE_PATH, file_name)
    symbol_name = file_name.split(".")[0]

    data = pd.read_csv(file_path)

    data["date"] = pd.to_datetime(data["date"])

    strategy = LastNProfitable(symbol=symbol_name, n_days=N_DAYS, data=data)

    transactions = strategy.run()

    output_path = os.path.join(OUTPUT_BASE_PATH, f"{symbol_name}.csv")

    transactions.to_csv(output_path)

    print(f"{symbol_name.ljust(15)} | Path: {output_path}")


def run_last_n_profitable():
    if not os.path.exists(OUTPUT_BASE_PATH):
        os.makedirs(OUTPUT_BASE_PATH, exist_ok=True)

    files = utils.get_files_in_dir(BASE_PATH)

    # Using utils to be able to switch to different implementations as needed
    executor = utils.get_executor()

    futures = []

    for file in files:
        futures.append(executor.submit(run_for_symbol, file))

    # results = []
    for future in futures:
        res = future.result()
        # results.append(res)


if __name__ == "__main__":
    run_last_n_profitable()
