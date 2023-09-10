from abc import ABC, abstractmethod
import pandas as pd


class BaseStrategy(ABC):
    def __init__(self, symbol, data):
        self._symbol = symbol
        self._data: pd.DataFrame = data
        self._transactions = pd.DataFrame(
            columns=["symbol", "timestamp", "price", "side"]
        )

    def run(self) -> pd.DataFrame:
        is_empty = False
        is_dataframe = isinstance(self._data, pd.DataFrame)

        if is_dataframe:
            if self._data.empty:
                is_empty = True
        elif not bool(self._data):
            is_empty = True

        if is_empty:
            raise Exception("No data was provided")

        transactions = self._run()

        if len(transactions) > 0:
            self._transactions = pd.DataFrame.from_dict(transactions)
            self._transactions.set_index("timestamp", drop=True, inplace=True)
        return self._transactions

    @abstractmethod
    def _run(self) -> list:
        pass
