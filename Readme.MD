This program intends to create an algorithm which trades with profit in Binance crypto market.

Part one, data ETL from binance API is currently working, extracting aggtrades and transforming the data, inserting in a MongoDB database.

In order to run the program you need MongoDB installation.

You also need and a valid binance api key and secret in /data_handling/data_helpers/secrets.py as 
BINANCE_API_KEY=XXXXX, BINANCE_API_SECRET=XXXXX

To run the program it is recommended to package it, but you can also use the IDE with input arguments.

To package the application go to the programs root dir and run:
'python3 ./package.py'

After this the application zipped to one single executable file and you can run the program with default arguments,

For this you just need python 3.10 and run 'python3 AlgoTrader.py' . 

