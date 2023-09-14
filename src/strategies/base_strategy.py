from abc import ABC, abstractmethod
import pandas as pd


class BaseStrategy(ABC):
    def __init__(self, symbol, data):
        self._symbol = symbol
        self._data: pd.DataFrame = data
        self._transactions = pd.DataFrame(
            columns=["symbol", "timestamp", "price", "side", "quantity"]
        )

        self._capital = 0

    def run(self) -> dict:
        is_empty = False
        is_dataframe = isinstance(self._data, pd.DataFrame)

        if is_dataframe:
            if self._data.empty:
                is_empty = True
        elif not bool(self._data):
            is_empty = True

        if is_empty:
            raise Exception("No data was provided")

        self._capital = self._data["close"].mean() * 100

        transactions = self._run()

        if len(transactions) > 0:
            self._transactions = pd.DataFrame.from_dict(transactions)

        self._transactions["side"] = self._transactions["side"].apply(lambda x: x.value)

        return {"transactions": self._transactions, "capital": self._capital}

    @abstractmethod
    def _run(self) -> list:
        pass
