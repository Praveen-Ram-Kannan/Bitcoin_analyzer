import time
from threading import Thread
import requests

file = '../data/bit_coin_data.csv'
url = 'https://storage.googleapis.com/zalora-interview-data/bitstampUSD_1-min_data_2012-01-01_to_2020-09-14.csv'


def request_api(header):
    '''
    Download data with the particular range mentioned in the header object
    :param header:
    :return: response code
    '''
    try:
        response = requests.get(url, headers=header, stream=True)
        open(f'{file}', 'ab').write(response.content)
        return response.status_code
    except requests.exceptions.RequestException as e:
        return e


def download_bitcoin_data():
    '''
    Since the size of the dataset is too large, if all the contents are fetched
     at once, the process will be slower
    Therefore multithreading is used, to capture chunks of data in parallel

    Approach :
    1. Split the entire data into 100 MB chunks.
    2. Since the file is around 280 MB, used 3 threads to run in parallel
    that fetches 100 MB of data each which improves the Scalability of the
    application.
    3. Headers used in the get request is created in such a way that the data
    is not duplicated.
    4. Once the chunk is fetched, the chunk is written to the output data file
    in parallel.
    :return:
    '''
    chunk_size = 104857600  # 100 MB chunks
    headers = []
    _start, _stop = 0, 0

    for x in range(3):
        _start = _stop
        _stop = chunk_size * (x + 1)
        headers.append({"Range": f"bytes={_start}-{_stop}"})

    for header in headers:
        t = Thread(target=request_api, args=(header,))
        t.start()
        t.join()
    return True


def main():
    '''
    Main method
    :return:
    '''
    start_time = time.time()
    download_bitcoin_data()
    request_time = time.time() - start_time
    print('Fetch completed')
    print(f'Time taken for get request is {request_time}')
