from datetime import datetime
import pandas as pd

import utils

from strategies.base_strategy import BaseStrategy
from constants import TransactionSide


class LastNConsecutive(BaseStrategy):
    def __init__(self, symbol, n_days=2, data: pd.DataFrame = pd.DataFrame.empty):
        super().__init__(symbol=symbol, data=data)

        self._n_days = n_days

    def _run(self) -> list:
        self._data["profitable"] = False

        # Part 1: Calculate the average of the last n days
        for i in range(self._n_days, len(self._data)):
            if i == len(self._data) - 1:
                continue

            profitable = True
            for j in range(1, self._n_days + 1):
                if not profitable:
                    break

                profitable = False
                if self._data.iloc[i - j]["close"] < self._data.iloc[i - j]["open"]:
                    profitable = True

            if self._data.iloc[i]["close"] < self._data.iloc[i]["open"]:
                profitable = False

            self._data.loc[i + 1, "profitable"] = profitable

        transactions = []

        for i in range(len(self._data)):
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
