import alpaca_markets as alpaca
import datetime

# List our variables
symbols = ["AAPL", "MSFT", "TSLA", "AMZN"]
max_number_of_candles = 1000
timeframe = "1hour"


# Function to run the trading bot
def auto_run_trading_bot():
    """
    Function to run the trading bot
    """
    # Print a Welcome message to the console
    print("Welcome to your Trading Bot!")
    # Set the end date to yesterday
    end_date = datetime.datetime.now() - datetime.timedelta(days=1) # Note that if you have a premium subscription you can remove this restriction
    # Set the start date to one year ago
    start_date = end_date - datetime.timedelta(days=365)
    # Get the symbols
    for symbol in symbols:
        # Save the symbol text
        symbol_text = symbol
        # Convert the symbol to a list
        symbol = [symbol]
        # Get the historic bars
        symbol_historical_data = alpaca.get_historic_bars(
            symbols=symbol, 
            timeframe=timeframe, 
            start_date=start_date, 
            end_date=end_date, 
            limit=max_number_of_candles
        )
        print(symbol_historical_data)



# Main function for the program
if __name__ == "__main__":
    # Run the trading bot
    auto_run_trading_bot()
