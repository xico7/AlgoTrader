This program intends to create an algorithm which trades with profit in Binance crypto market, after deducting fees.

The only part which is currently working is the data gathering and transformation through Binance API.

In order to run this program you need MongoDB installed and running, you also need a binance api key and secret in /data_handling/data_helpers/secrets.py (BINANCE_API_KEY=XXXXX, BINANCE_API_SECRET=XXXXX).

To insert all trades separated by each symbol in a MongoDB database run the program with the following arguments:

python3 ./package.py

After this will you have the application zipped to one single executable file and you can run the data aggregators.

python3 AlgoTrader.py ws-trades (gathers data from the past, it also contains a live data aggregators for later stages)

python3 AlgoTrader.py parse-trades-fund ( transforms the data in a price/volume -> time relationship for all relevant symbols)
python3 AlgoTrader.py parse-trades-ten-seconds ( transforms the data in a price/volume -> time relationship for the symbols which comprise the fund (more marketcap)).


Next steps:

transform_trade_data, uses the parsed data in a way to, for example, construct "graphical" information on how much a 
symbol was traded in each price range for one day, and relate that with fund data to technical analysis a simple process.