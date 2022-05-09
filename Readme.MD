This program intends to create an algorithm which trades with profit in Binance crypto market, after deducting fees.

The only part which is currently working is the data gathering and transformation through Binance API.

In order to run this program you need MongoDB installed and running, after that run the program with argparse commands.

To insert all trades separated by each symbol in a MongoDB database run the program with the following arguments:

python3 ./main.py ws-trades

After this you can also transform those trade information ( Price and volume ) into more useful data, like so:

python3 ./main.py transform-trade-data --transform-trade-data-timeframe-in-secs 60 --transform-trade-data-interval-in-secs 20 --transform-trade-data-db-name aggtrade_data

TODO: explain timeframe and data interval.