from datetime import datetime
import pandas as pd

import os

from strategies.base_strategy import BaseStrategy
from constants import TransactionSide


class LastNProfitable(BaseStrategy):
    def __init__(self, symbol, n_days=2, data: pd.DataFrame = pd.DataFrame.empty):
        super().__init__(symbol=symbol, data=data)

        self._n_days = n_days

    def run(self) -> pd.DataFrame:
        if self._data.empty:
            raise Exception("No data was provided")

        # Part 1: Calculate the average of the last n days

        self._data["profitable"] = False

        days_passed = 0

        for i, row in self._data.iterrows():
            days_passed += 1
            if days_passed < self._n_days:
                continue

            profitable = self._data.iloc[i]["close"] > self._data.iloc[i]["open"]

            self._data.loc[i, "profitable"] = profitable

            if not profitable:
                days_passed = 0

        for i, row in self._data.iterrows():
            if not self._data.iloc[i]["profitable"]:
                continue

            new_transactions = pd.DataFrame.from_dict(
                [
                    {
                        "symbol": self._symbol,
                        "timestamp": self._data.iloc[i]["date"].replace(
                            hour=9, minute=15, second=0
                        ),
                        "price": self._data.iloc[i]["open"],
                        "side": TransactionSide.BUY,
                    },
                    {
                        "symbol": self._symbol,
                        "timestamp": self._data.iloc[i]["date"].replace(
                            hour=15, minute=30, second=0
                        ),
                        "price": self._data.iloc[i]["close"],
                        "side": TransactionSide.SELL,
                    },
                ]
            )

            if self._transactions.empty:
                self._transactions = new_transactions
                continue

            self._transactions = pd.concat([self._transactions, new_transactions])

        self._transactions.set_index("timestamp", drop=True, inplace=True)

        return self._transactions


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
