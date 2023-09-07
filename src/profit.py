import pandas as pd

import utils
from constants import TransactionSide


def get_profit_for_symbol(file, dir):
    file_path = f"{dir}/{file}"
    df = pd.read_csv(file_path)

    # print(data)
    # quit()

    net = 1
    data = df.to_dict("records")

    for i in range(len(data) // 2):
        buy = data[i * 2]
        sell = data[i * 2 + 1]

        try:
            assert buy["side"] == "TransactionSide.BUY"
            assert sell["side"] == "TransactionSide.SELL"
        except AssertionError:
            print(f"AssertionError for {file}")
            continue

        net *= sell["price"] / buy["price"]

    return net


def get_profit(dir):
    files = utils.get_files_in_dir(dir)

    # Using utils to be able to switch to different implementations as needed
    executor = utils.get_executor()

    futures = []

    for file in files:
        futures.append(executor.submit(get_profit_for_symbol, file, dir))
        # break

    results = []
    for future in futures:
        res = future.result()
        results.append(res)

    print(f"AVG: {round(100 * (sum(results) / len(results) - 1), 2)}%")
    print(f"MIN: {round(100 * (min(results) - 1), 2)}%")
    print(f"MAX: {round(100 * (max(results) - 1), 2)}%")


if __name__ == "__main__":
    get_profit("output/last_2_days_profitable")
