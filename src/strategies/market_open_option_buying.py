from datetime import datetime
import math
import pandas as pd
from datetime import datetime

from strategies.base_strategy import BaseStrategy
from constants import TransactionSide

import utils


class MarketOpenOptionBuying(BaseStrategy):
    def __init__(self, symbol, data: pd.DataFrame = pd.DataFrame.empty):
        super().__init__(symbol=symbol, data=data)

    def _run(self) -> list:
        buy_row = self._data[
            self._data["time"].dt.time == datetime(1970, 1, 1, 9, 15).time()
        ].iloc[0]
        buy_price = buy_row["open"]
        quantity = utils.get_quantity(buy_price, self._capital)

        sell_row = self._data[
            self._data["time"].dt.time == datetime(1970, 1, 1, 10, 0).time()
        ].iloc[0]
        sell_price = sell_row["close"]

        return [
            {
                "symbol": self._symbol,
                "timestamp": buy_row["time"],
                "price": buy_price,
                "quantity": quantity,
                "side": TransactionSide.BUY,
            },
            {
                "symbol": self._symbol,
                "timestamp": sell_row["time"],
                "price": sell_price,
                "quantity": quantity,
                "side": TransactionSide.SELL,
            },
        ]
