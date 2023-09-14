from datetime import datetime
import math
import pandas as pd
from random import randint

from strategies.base_strategy import BaseStrategy
from constants import TransactionSide

import utils


class RandomBuyingSelling(BaseStrategy):
    def __init__(self, symbol, data: pd.DataFrame = pd.DataFrame.empty):
        super().__init__(symbol=symbol, data=data)

    def _run(self) -> list:
        transactions = []
        quantity_open = 0
        for i, row in self._data.iterrows():
            buy = randint(0, 1)
            sell = randint(0, 1)

            if buy:
                is_close = randint(0, 1)
                price = 0
                timestamp = None
                if is_close:
                    price = row["close"]
                    timestamp = row["date"].replace(hour=15, minute=30, second=0)
                else:
                    price = row["open"]
                    timestamp = row["date"].replace(hour=9, minute=15, second=0)

                max_quantity = utils.get_quantity(price, self._capital)
                quantity = randint(1, max_quantity)

                transactions.append(
                    {
                        "symbol": self._symbol,
                        "timestamp": timestamp,
                        "price": price,
                        "quantity": quantity,
                        "side": TransactionSide.BUY,
                    }
                )
                quantity_open += quantity

            # For each symbol, we only want to sell if there is some quantity that exists
            if sell:
                is_close = randint(0, 1)
                price = 0
                timestamp = None
                if is_close:
                    price = row["close"]
                    timestamp = row["date"].replace(hour=15, minute=30, second=0)
                else:
                    price = row["open"]
                    timestamp = row["date"].replace(hour=9, minute=15, second=0)

                max_quantity = utils.get_quantity(price, self._capital)
                quantity = randint(1, max_quantity)

                transactions.append(
                    {
                        "symbol": self._symbol,
                        "timestamp": timestamp,
                        "price": price,
                        "quantity": quantity,
                        "side": TransactionSide.SELL,
                    }
                )

                quantity_open -= quantity

        if quantity_open != 0:
            date = self._data.iloc[-1]["date"].replace(hour=15, minute=30, second=0)
            side = TransactionSide.SELL if quantity_open > 0 else TransactionSide.BUY

            transactions.append(
                {
                    "symbol": self._symbol,
                    "timestamp": date,
                    "price": self._data.iloc[-1]["close"],
                    "quantity": abs(quantity_open),
                    "side": side,
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

    strategy = RandomBuyingSelling("BANKNIFTY", n_days=2, data=data)
    results = strategy.run()

    print(results)
