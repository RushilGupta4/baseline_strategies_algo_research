import pandas as pd


class LastNProfitable:
    def __init__(self, symbol, n_days=2, data: pd.DataFrame = pd.DataFrame.empty):
        self._symbol = symbol
        self._n_days = n_days
        self._data = data
        self._transactions = pd.DataFrame(
            columns=["symbol", "date", "buy_price", "sell_price"]
        )

    def run(self):
        if self._data.empty:
            raise Exception("No data was provided")

        # Part 1: Calculate the average of the last n days

        days_passed = 0

        for i, row in self._data.iterrows():
            days_passed += 1
            if days_passed < self._n_days:
                self._data.loc[i, "profitable"] = False
                continue

            self._data.loc[i, "profitable"] = (
                self._data.iloc[i]["close"] > self._data.iloc[i]["open"]
            )

            days_passed = 0

        for i, row in self._data.iterrows():
            if not self._data.iloc[i]["profitable"]:
                continue

            # self._transactions.loc[len(self._transactions)] = pd.Series(
            #     data={
            #         "symbol": self._symbol,
            #         "date": self._data.iloc[i]["time"],
            #         "buy_price": self._data.iloc[i]["open"],
            #         "sell_price": self._data.iloc[i]["close"],
            #     }
            # )

            self._transactions = pd.concat(
                [
                    self._transactions,
                    pd.DataFrame.from_dict(
                        [
                            {
                                "symbol": self._symbol,
                                "date": self._data.iloc[i]["time"],
                                "buy_price": self._data.iloc[i]["open"],
                                "sell_price": self._data.iloc[i]["close"],
                            }
                        ]
                    ),
                ]
            )
            # print(self._data.iloc[i])

        return self._transactions


if __name__ == "__main__":
    data = pd.DataFrame.from_dict(
        [
            {
                "time": "2021-01-01",
                "open": 10,
                "close": 20,
            },
            {
                "time": "2021-01-02",
                "open": 20,
                "close": 30,
            },
            {
                "time": "2021-01-03",
                "open": 30,
                "close": 40,
            },
            {
                "time": "2021-01-04",
                "open": 40,
                "close": 38,
            },
            {
                "time": "2021-01-05",
                "open": 38,
                "close": 50,
            },
        ],
    )

    last_n_profitable = LastNProfitable("BANKNIFTY", n_days=2, data=data)
    last_n_profitable.run()
