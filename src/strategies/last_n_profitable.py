from datetime import datetime
import pandas as pd

import utils

from strategies.base_strategy import BaseStrategy
from constants import TransactionSide


class LastNProfitable(BaseStrategy):
    def __init__(self, symbol, n_days=2, data: pd.DataFrame = pd.DataFrame.empty):
        super().__init__(symbol=symbol, data=data)

        self._n_days = n_days

    def _run(self) -> list:
        self._data["profitable"] = False

        # Part 1: Calculate the average of the last n days
        days_passed = 0

        for i, row in self._data.iterrows():
            days_passed += 1
            if days_passed < self._n_days:
                continue

            if i == len(self._data) - 1:
                continue

            profitable = self._data.iloc[i]["close"] > self._data.iloc[i]["open"]

            self._data.loc[i + 1, "profitable"] = profitable

            if not profitable:
                days_passed = 0

        transactions = []

        for i, row in self._data.iterrows():
            if not self._data.iloc[i]["profitable"]:
                continue

            open = self._data.iloc[i]["open"]
            quantity = utils.get_quantity(open, self._capital)

            transactions.append(
                {
                    "symbol": self._symbol,
                    "timestamp": self._data.iloc[i]["date"].replace(
                        hour=9, minute=15, second=0
                    ),
                    "price": open,
                    "quantity": quantity,
                    "side": TransactionSide.BUY,
                }
            )

            close = self._data.iloc[i]["close"]

            transactions.append(
                {
                    "symbol": self._symbol,
                    "timestamp": self._data.iloc[i]["date"].replace(
                        hour=15, minute=30, second=0
                    ),
                    "price": close,
                    "quantity": quantity,
                    "side": TransactionSide.SELL,
                }
            )

        return transactions


if __name__ == "__main__":
    data = pd.DataFrame.from_dict(
        [
            {
                "date": datetime(year=2021, month=1, day=1),
                "open": 10,
                "close": 20,
            },
            {
                "date": datetime(year=2021, month=1, day=2),
                "open": 20,
                "close": 30,
            },
            {
                "date": datetime(year=2021, month=1, day=3),
                "open": 30,
                "close": 40,
            },
            {
                "date": datetime(year=2021, month=1, day=4),
                "open": 37,
                "close": 38,
            },
            {
                "date": datetime(year=2021, month=1, day=5),
                "open": 38,
                "close": 50,
            },
        ],
    )

    last_n_profitable = LastNProfitable("BANKNIFTY", n_days=2, data=data)
    results = last_n_profitable.run()

    print(results)
