from datetime import datetime
import pandas as pd
from random import randint

from strategies.base_strategy import BaseStrategy
from constants import TransactionSide


class RandomBuyingSellingWithSL(BaseStrategy):
    def __init__(self, symbol, data: pd.DataFrame = pd.DataFrame.empty):
        super().__init__(symbol=symbol, data=data)

    def run(self) -> pd.DataFrame:
        if self._data.empty:
            raise Exception("No data was provided")

        current_sl = 0
        last_buy = 0

        for i, row in self._data.iterrows():
            buy = randint(0, 1)
            sell = randint(0, 1)

            if self._transactions.empty:
                last_transaction = TransactionSide.SELL
            else:
                last_transaction = self._transactions.iloc[-1]["side"]

            new_transactions = []
            if buy and last_transaction == TransactionSide.SELL:
                new_transactions.append(
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
                current_sl = self._data.iloc[i]["low"] - 2 * (
                    self._data.iloc[i]["high"] - (self._data.iloc[i]["close"])
                )
                last_buy = self._data.iloc[i]["open"]

            if sell and last_transaction == TransactionSide.BUY:
                # if sl was hit in the day, sell on sl price
                if self._data.iloc[i]["low"] < current_sl:
                    new_transactions.append(
                        {
                            "symbol": self._symbol,
                            "timestamp": self._data.iloc[i]["date"].replace(
                                hour=15, minute=30, second=0
                            ),
                            "price": current_sl,
                            "side": TransactionSide.SELL,
                        }
                    )
                    last_transaction = TransactionSide.SELL

                # only buy if price > buy price
                elif self._data.iloc[i]["close"] > last_buy:
                    new_transactions.append(
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

            new_transactions = pd.DataFrame.from_dict(new_transactions)

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
