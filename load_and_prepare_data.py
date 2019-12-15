import pandas as pd
from pprint import pprint
from pymongo import MongoClient
client = MongoClient()
collection = client.bucephalus.stock_prices
symbols = []
for symbol in collection.distinct("reqId"):
    symbols.append(symbol)

corr_table = { "pair": [], "pearson": [], "kendall": [] }
for i in range(len(symbols)):
    a_price_data = { "close": [], "high": [], "low": [], "open": [], "volume": [], "date": [] }
    for row in collection.find({"reqId": symbols[i]}):
        a_price_data["close"].append(row["close"])
        a_price_data["high"].append(row["high"])
        a_price_data["low"].append(row["low"])
        a_price_data["open"].append(row["open"])
        a_price_data["volume"].append(row["volume"])
        a_price_data["date"].append(row["date"])

    a_data = pd.DataFrame(a_price_data).set_index("date")
    a_data.sort_index(inplace=True)

    for j in range(i + 1, len(symbols)):
        b_price_data = { "close": [], "high": [], "low": [], "open": [], "volume": [], "date": [] }
        for row in collection.find({"reqId": symbols[j]}):
            b_price_data["close"].append(row["close"])
            b_price_data["high"].append(row["high"])
            b_price_data["low"].append(row["low"])
            b_price_data["open"].append(row["open"])
            b_price_data["volume"].append(row["volume"])
            b_price_data["date"].append(row["date"])

        b_data = pd.DataFrame(b_price_data).set_index("date")
        b_data.sort_index(inplace=True)
        corr_table["pair"].append(symbols[i] + " " + symbols[j])
        corr_table["pearson"].append(a_data.corrwith(b_data, drop=True)["close"])
        corr_table["kendall"].append(a_data.corrwith(b_data, drop=True, method="kendall")["close"])
        print (symbols[i] + " " + symbols[j])

corr_data = pd.DataFrame(corr_table).set_index("pair")
corr_data.to_csv("correlations.csv")
