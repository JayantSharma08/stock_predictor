import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging


class StockDataProcessor:
    """
    A class to handle stock data collection and processing using yfinance API.
    """

    def __init__(self, data_dir="data"):
        """
        Initialize the StockDataProcessor with a data directory.

        Args:
            data_dir (str): Directory to store the CSV files
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def fetch_stock_data(self, ticker, days_back):
        """
        Fetch stock data for a given ticker and time period.

        Args:
            ticker (str): Stock ticker symbol
            days_back (int): Number of days to look back

        Returns:
            pandas.DataFrame: Retrieved stock data
        """
        try:
            end_date = datetime.today()
            start_date = end_date - timedelta(days=days_back)

            self.logger.info(f"Fetching data for {ticker} from {start_date.date()} to {end_date.date()}")

            # Fetch data using yfinance
            stock = yf.Ticker(ticker)
            hist_data = stock.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d')
            )

            # Get analyst targets if available
            try:
                target_data = stock.info.get('targetMeanPrice', None)
                if target_data:
                    hist_data['AnalystTargetPrice'] = target_data
            except Exception as e:
                self.logger.warning(f"Could not fetch analyst targets: {e}")

            return hist_data

        except Exception as e:
            self.logger.error(f"Error fetching data for {ticker}: {e}")
            raise

    def process_data(self, data):
        """
        Process the raw stock data to prepare it for modeling.

        Args:
            data (pandas.DataFrame): Raw stock data

        Returns:
            pandas.DataFrame: Processed data
        """
        try:
            processed_data = data.copy()

            # Add technical indicators
            processed_data['MA5'] = processed_data['Close'].rolling(window=5).mean()
            processed_data['MA20'] = processed_data['Close'].rolling(window=20).mean()
            processed_data['Daily_Return'] = processed_data['Close'].pct_change()
            processed_data['Volatility'] = processed_data['Daily_Return'].rolling(window=20).std()

            # Drop any NaN values resulting from calculations
            processed_data.dropna(inplace=True)

            return processed_data

        except Exception as e:
            self.logger.error(f"Error processing data: {e}")
            raise

    def save_data(self, data, ticker):
        """
        Save the processed data to a CSV file.

        Args:
            data (pandas.DataFrame): Processed stock data
            ticker (str): Stock ticker symbol
        """
        try:
            filename = self.data_dir / f"{ticker}_stock_data.csv"
            data.to_csv(filename)
            self.logger.info(f"Data saved to {filename}")

        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
            raise

    def run_pipeline(self, ticker, days_back):
        """
        Run the complete data processing pipeline.

        Args:
            ticker (str): Stock ticker symbol
            days_back (int): Number of days to look back

        Returns:
            pandas.DataFrame: Processed stock data
        """
        try:
            # Fetch data
            raw_data = self.fetch_stock_data(ticker, days_back)

            # Process data
            # processed_data = self.process_data(raw_data)

            # Save data
            self.save_data(raw_data, ticker)

            return raw_data

        except Exception as e:
            self.logger.error(f"Pipeline failed for {ticker}: {e}")
            raise


def main():
    # Initialize the processor
    processor = StockDataProcessor(data_dir="stock_data")

    # Example usage
    ticker = "AAPL"
    days_back = 18  # Approximately 5 years

    try:
        # Run the pipeline
        data = processor.run_pipeline(ticker, days_back)
        print(f"Successfully processed data for {ticker}")
        print("\nFirst few rows of processed data:")
        print(data.head())

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()