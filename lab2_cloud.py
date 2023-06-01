import boto3, botocore.exceptions
import matplotlib.pyplot as plt
import pandas


def connect_to_bucket():
    session = boto3.Session()
    s3 = session.client('s3')
    return s3


def Read_call():
    format = pandas.DataFrame({'USD': [], 'EUR': [], "Date": []})
    start_date = '1/1/2021'
    end_date = '12/31/2021'
    cycle = (pandas.Series(pandas.date_range(start=start_date, end=end_date, freq='d'))).dt.strftime('%Y%m%d')
    for i in cycle:
        format_t = pandas.read_json(f'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?date={i}&json')
        format_t = pandas.DataFrame({'USD': format_t[format_t['cc'] == "USD"]['rate'].values,
                                     'EUR': format_t[format_t['cc'] == "EUR"]['rate'].values,
                                     "Date": format_t[format_t['cc'] == "USD"]['exchangedate'].values})
        format = pandas.concat([format, format_t], axis=0)
    format = format.reset_index(drop=True)
    format.to_csv("Data_for_diagramm3.csv", index=False)


def upload_to_bucket(s3, filename, bucketname):
    with open(filename, "rb") as f:
        s3.upload_fileobj(f, bucketname, filename)


def download_from_bucket(s3, filename, bucketname):
    s3.download_file(bucketname, filename, filename)


def Create_diagramm(format):
    format.plot(x='Date', y=['USD', 'EUR'], figsize=(15, 7), title="UAH", fontsize=10)
    plt.savefig('chart3.png')


conn = connect_to_bucket()
try:
    download_from_bucket(conn, 'Data_for_diagramm3.csv', 'zamlab2')
except botocore.exceptions.ClientError:
    Read_call()
    upload_to_bucket(conn, 'Data_for_diagramm3.csv', 'zamlab2')

cr = pandas.read_csv("Data_for_diagramm3.csv")
Create_diagramm(cr)
upload_to_bucket(conn, 'chart3.png', 'zamlab2')
