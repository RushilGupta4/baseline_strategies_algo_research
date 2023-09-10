from datetime import datetime
import pandas as pd
from random import randint

from strategies.base_strategy import BaseStrategy
from constants import TransactionSide


class RandomBuyingSelling(BaseStrategy):
    def __init__(self, symbol, data: pd.DataFrame = pd.DataFrame.empty):
        super().__init__(symbol=symbol, data=data)

    def _run(self) -> list:
        transactions = []
        for i, row in self._data.iterrows():
            buy = randint(0, 1)
            sell = randint(0, 1)

            if self._transactions.empty:
                last_transaction = TransactionSide.SELL
            else:
                last_transaction = self._transactions.iloc[-1]["side"]

            if buy and last_transaction.value == TransactionSide.SELL.value:
                transactions.append(
                    {
                        "symbol": self._symbol,
                        "timestamp": self._data.iloc[i]["date"].replace(
                            hour=9, minute=15, second=0
                        ),
                        "price": self._data.iloc[i]["open"],
                        "side": TransactionSide.BUY,
                    }
                )
                last_transaction = TransactionSide.BUY

            if sell and last_transaction.value == TransactionSide.BUY.value:
                transactions.append(
                    {
                        "symbol": self._symbol,
                        "timestamp": self._data.iloc[i]["date"].replace(
                            hour=15, minute=30, second=0
                        ),
                        "price": self._data.iloc[i]["close"],
                        "side": TransactionSide.SELL,
                    }
                )
                last_transaction = TransactionSide.SELL

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
