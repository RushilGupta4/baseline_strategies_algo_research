import utils
import pandas as pd
import os

BASE_PATH = os.path.join("data", "zerodha_d1")


def run_for_symbol(strategy_obj, file_name, output_dir, **kwargs):
    file_path = os.path.join(BASE_PATH, file_name)
    symbol_name = file_name.split(".")[0]

    data = pd.read_csv(file_path)

    data["date"] = pd.to_datetime(data["date"])

    strategy = strategy_obj(symbol=symbol_name, **kwargs, data=data)

    transactions = strategy.run()
    output_path = os.path.join(output_dir, f"{symbol_name}.csv")
    transactions.to_csv(output_path)

    print(f"{symbol_name.ljust(15)} | Path: {output_path}")


def run_strategy(strategy, output_dir, **strategy_kwargs):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    files = utils.get_files_in_dir(BASE_PATH)

    # Using utils to be able to switch to different implementations as needed
    executor = utils.get_executor()

    futures = []

    for file in files:
        futures.append(
            executor.submit(
                run_for_symbol, strategy, file, output_dir, **strategy_kwargs
            )
        )

    # results = []
    for future in futures:
        res = future.result()
        # results.append(res)


if __name__ == "__main__":
    run_strategy()
