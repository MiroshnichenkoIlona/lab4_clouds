import requests
import boto3
import io
import json
from matplotlib import pyplot as plt


def credentials():
    with open('credentials.json', 'r') as file:
        return json.load(file)


def get_currency(currency):
    link = requests.get(f'https://bank.gov.ua/NBU_Exchange/exchange_site?start=20210101&end=20211231&valcode={currency}'
                        f'&sort=exchangedate&order=desc&json')
    return link.json()


def final_data(currency):
    final_str = ''
    raw_currency = get_currency(currency)
    for currency_i in raw_currency:
        final_str += currency_i["exchangedate"] + ', ' + str(currency_i["rate"]) + '\n'
    return final_str


def create_csv(currency, final_str):
    with open(f"{currency}.csv", "w") as file:
        file.write(final_str)


def upload_csv(test_file, bucket_name, file):
    test_file = io.BytesIO(test_file)
    s3_client.upload_fileobj(test_file, bucket_name, file)


def read_bucket(currency):
    object = s3_read.Object(bucket_name, f'{currency}.csv')
    file_content = object.get()['Body'].read().decode('utf-8')
    return file_content


def chart_data(currency):
    data_for_x = []
    data_for_y = []
    final_data_for_chart = read_bucket(currency)[:-1]
    for row in final_data_for_chart.split('\n'):
        row = row.split(', ')
        data_for_x.append(row[0])
        data_for_y.append(float(row[1].replace('\r', '')))
    data_for_x.reverse()
    data_for_y.reverse()
    return data_for_x, data_for_y


def chart():
    x, y = chart_data('USD')
    _, y1 = chart_data('EUR')
    plt.xticks(range(0, len(x))[::31], rotation='vertical')
    plt.plot(x, y, label="USD")
    plt.plot(x, y1, label="EUR")
    plt.xlabel('Date')
    plt.ylabel('Currency')
    plt.title('Currency rate')
    plt.legend()
    plt.show()
    plt.savefig('chart.png')
    s3_client.upload_file('chart.png', bucket_name, 'chart.png')


keys = credentials()

s3_client = boto3.client('s3', region_name='us-east-1', aws_access_key_id=keys.get('aws_access_key_id'),
                         aws_secret_access_key=keys.get('aws_secret_access_key'))
bucket_name = 'data11111'
s3_read = boto3.resource('s3', aws_access_key_id=keys.get('aws_access_key_id'),
                         aws_secret_access_key=keys.get('aws_secret_access_key'))

create_csv('USD', final_data('USD'))
create_csv('EUR', final_data('EUR'))

test_file = open("USD.csv", "rb").read()
upload_csv(test_file, bucket_name, "USD.csv")

test_file = open("EUR.csv", "rb").read()
upload_csv(test_file, bucket_name, "EUR.csv")

print(read_bucket('USD'))

chart()
