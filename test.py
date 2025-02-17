from pybit.unified_trading import HTTP
session = HTTP(testnet=True)
print(session.get_tickers(
    category="inverse",
    symbol="BTCUSD",
))