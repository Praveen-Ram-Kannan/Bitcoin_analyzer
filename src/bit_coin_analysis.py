from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from src.data_extraction import download_bitcoin_data

input_data_file = '../data/bit_coin_data.csv'
output_data_file = '../data/SuccessfulTradesDF.csv'


def spark_initializer():
    '''
    Initializes spark context and sql context
    :return: sqlcontext object
    '''
    sc = SparkContext("local", "Bit Coin Analyzer")
    sqlContext = SQLContext(sc)
    return sqlContext


def bit_coin_analyzer(sqlContext):
    '''
    Analyzes and finds successful trades that can happen with the given Bitcoin dataset.

    :param sqlContext: spark sqlcontext variable used for doing transformations to the dataset

    :return: A dataframe consisting of all the successful trades that can happen using the above
     mentioned approach provided the given Bitcoin dataset
    '''

    # read the csv file into a spark dataframe and dropping all the NaN/NULL values
    df = sqlContext.read.csv(input_data_file, header='true',
                             inferSchema='true').dropna('any')

    df.createOrReplaceTempView('Bitcoin_View')

    # Transformation 1 - Transform Timestamp to date and time value
    BitcoinDF = sqlContext.sql('select from_unixtime(Timestamp) as `dateTime`, '
                               'High, Low from Bitcoin_View')

    # Transformation 2 - Extract date from date and time column
    BitcoinDF = BitcoinDF.withColumn('dateColumn', BitcoinDF['dateTime'].cast('date')).drop('dateTime')

    BitcoinDF.createOrReplaceTempView('Bitcoin_temp_view')

    # Selecting maximum and minimum value for each day
    BitcoinDF = sqlContext.sql('select dateColumn, max(High) as High, min(Low) as Low '
                               'from Bitcoin_temp_view group by dateColumn order by dateColumn')

    BitcoinDF.createOrReplaceTempView('Bitcoin_temp2_view')

    # Gather All possible days where a successful trade can happen
    SuccessfulTradesDF_temp = sqlContext.sql('select a.dateColumn as Buying_date, '
                                             'a.High as Buy_High, a.Low as Buy_Low, '
                                             'b.dateColumn as Selling_date, '
                                             'b.High as Sell_high, b.Low as Sell_Low '
                                             'from Bitcoin_temp2_view a '
                                             'join Bitcoin_temp2_view b '
                                             'on a.dateColumn < b.dateColumn')

    SuccessfulTradesDF_temp.createOrReplaceTempView('successful_trades_view')

    # Union of all successful trades data that is gathered using the above mentioned 4 assumptions
    SuccessfulTradesDF = sqlContext.sql('select Buying_date, Buy_price, Selling_date, Sell_price, '
                                        'cast((((Sell_price - Buy_price)/Buy_price)*100) as decimal(13,2)) as '
                                        'ROI_percentage from '
                                        '((select Buying_date, Buy_Low as Buy_price, Selling_date, Sell_high as Sell_price '
                                        'from successful_trades_view '
                                        'where Buy_Low<Sell_High) '
                                        'union '
                                        '(select Buying_date, Buy_Low as Buy_price, Selling_date, Sell_low as Sell_price '
                                        'from successful_trades_view '
                                        'where Buy_Low<Sell_Low) '
                                        'union '
                                        '(select Buying_date, Buy_High as Buy_price, Selling_date, Sell_high as Sell_price '
                                        'from successful_trades_view '
                                        'where Buy_High<Sell_high) '
                                        'union '
                                        '(select Buying_date, Buy_High as Buy_price, Selling_date, Sell_Low as Sell_price '
                                        'from successful_trades_view '
                                        'where Buy_High<Sell_Low)) '
                                        'order by Buying_date, Selling_date')

    return SuccessfulTradesDF


def write_sparkdf_to_csv(df):
    '''
    Write the dataframe which contains the successful trades to an output csv file
    :param df:
    :return:
    '''
    df.write.csv(output_data_file)
    return True


def main():
    '''
    Main method
    :return:
    '''
    try:
        download_status = download_bitcoin_data()
        if download_status == True:
            sqlContext = spark_initializer()
            SuccessfulTradesDF = bit_coin_analyzer(sqlContext)
            write_sparkdf_to_csv(SuccessfulTradesDF)
        else:
            print('Bitcoin data download is not successful')
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
