from abc import ABC, abstractmethod
import pandas as pd


class BaseStrategy(ABC):
    def __init__(self, symbol, data):
        self._symbol = symbol
        self._data: pd.DataFrame = data
        self._transactions = pd.DataFrame(
            columns=["symbol", "timestamp", "price", "side"]
        )

    @abstractmethod
    def run(self) -> pd.DataFrame:
        pass
