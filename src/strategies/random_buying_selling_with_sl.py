from datetime import datetime
import pandas as pd
from random import randint

from strategies.base_strategy import BaseStrategy
from constants import TransactionSide

import utils


class RandomBuyingSellingWithSL(BaseStrategy):
    def __init__(self, symbol, data: pd.DataFrame = pd.DataFrame.empty):
        super().__init__(symbol=symbol, data=data)

    def _run(self) -> list:
        current_sl = 0
        last_buy = 0
        current_quantity = 0

        transactions = []

        # Base case
        last_transaction = TransactionSide.SELL

        for i, row in self._data.iterrows():
            buy = randint(0, 1)
            sell = randint(0, 1)

            if buy and last_transaction.value == TransactionSide.SELL.value:
                is_close = randint(0, 1)
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
                        "price": row["open"],
                        "quantity": quantity,
                        "side": TransactionSide.BUY,
                    }
                )

                last_transaction = TransactionSide.BUY

                # SL
                current_sl = row["low"] - 2 * (row["high"] - (row["low"]))
                last_buy = row["open"]
                current_quantity = quantity

            if sell and last_transaction.value == TransactionSide.BUY.value:
                is_close = randint(0, 1)
                if is_close:
                    price = row["close"]
                    timestamp = row["date"].replace(hour=15, minute=30, second=0)
                else:
                    price = row["open"]
                    timestamp = row["date"].replace(hour=9, minute=15, second=0)

                # if sl was hit in the day, sell on sl price
                if row["low"] < current_sl:
                    transactions.append(
                        {
                            "symbol": self._symbol,
                            "timestamp": self._data.iloc[i]["date"].replace(
                                hour=15, minute=30, second=0
                            ),
                            "price": current_sl,
                            "quantity": current_quantity,
                            "side": TransactionSide.SELL,
                        }
                    )
                    last_transaction = TransactionSide.SELL

                # If random sell signal is generated, and price > entry price
                elif price > last_buy:
                    transactions.append(
                        {
                            "symbol": self._symbol,
                            "timestamp": timestamp,
                            "price": price,
                            "quantity": current_quantity,
                            "side": TransactionSide.SELL,
                        }
                    )
                    last_transaction = TransactionSide.SELL

        # We need to sell here
        if last_transaction.value == TransactionSide.BUY.value:
            row = self._data.iloc[-1]
            transactions.append(
                {
                    "symbol": self._symbol,
                    "timestamp": row["date"].replace(hour=15, minute=30, second=0),
                    "price": row["close"],
                    "quantity": current_quantity,
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

    strategy = RandomBuyingSellingWithSL("BANKNIFTY", n_days=2, data=data)
    results = strategy.run()

    print(results)
