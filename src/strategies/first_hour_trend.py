from datetime import datetime
import pandas as pd

import utils

from strategies.base_strategy import BaseStrategy
from constants import TransactionSide


class FirstHourTrend(BaseStrategy):
    def __init__(self, symbol, n_days=2, data: pd.DataFrame = pd.DataFrame.empty):
        super().__init__(symbol=symbol, data=data)

        self._n_days = n_days

    def _run(self) -> list:
        last_date = None
        transactions = []
        for i, row in self._data.iterrows():
            curr_date = row["date"].date()
            if last_date == curr_date:
                continue

            last_date = curr_date

            start = row["date"].replace(hour=1, minute=0)
            end = row["date"].replace(hour=23, minute=0)

            today_candles_mask = (self._data["date"] >= start) & (
                self._data["date"] <= end
            )

            today_candles: pd.DataFrame = self._data.loc[today_candles_mask]
            today_candles.reset_index(inplace=True, drop=True)

            if len(today_candles) <= 2:
                print(
                    f"\n\n\nError for {self._symbol}\nCandles:\n{today_candles}\n\n\n"
                )

            first_hour_candle = today_candles.iloc[0]
            second_hour_candle = today_candles.iloc[1]
            last_candle = today_candles.iloc[-1]

            # Green candle
            profitable = first_hour_candle["close"] > first_hour_candle["open"]

            if not profitable:
                continue

            open = second_hour_candle["open"]
            quantity = utils.get_quantity(open, self._capital)

            transactions.append(
                {
                    "symbol": self._symbol,
                    "timestamp": row["date"].replace(hour=10, minute=15, second=0),
                    "price": open,
                    "quantity": quantity,
                    "side": TransactionSide.BUY,
                }
            )

            close = last_candle["close"]

            transactions.append(
                {
                    "symbol": self._symbol,
                    "timestamp": row["date"].replace(hour=15, minute=30, second=0),
                    "price": close,
                    "quantity": quantity,
                    "side": TransactionSide.SELL,
                }
            )

        return transactions
