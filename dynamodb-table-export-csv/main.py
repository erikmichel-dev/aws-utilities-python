import boto3
import csv
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("table_name", help="Table name to export")
parser.add_argument('--attributes', type=str, help="A comma-separated list of attribute names to retrieve from the DynamoDB table. If specified, the scan will return only these attributes. If not provided, all attributes will be returned.")
parser.add_argument('--limit', type=int, help="Maximum number of items to return from the scan operation. Default is 100.")
parser.add_argument('--profile', type=str, help="AWS CLI profile to use for authentication. Defaults to 'default' if not specified.")
args = parser.parse_args()

session = boto3.session.Session(
    profile_name=args.profile if args.profile else 'default'
)
dynamodb = session.resource('dynamodb')
table = dynamodb.Table(args.table_name)

scan_params = {}
scan_params['Limit'] = args.limit if args.limit else 100
if args.attributes:
    scan_params['Select'] = 'SPECIFIC_ATTRIBUTES'
    scan_params['ProjectionExpression'] = args.attributes
else:
    scan_params['Select'] = 'ALL_ATTRIBUTES'

response = table.scan(**scan_params)
data = response['Items']

while 'LastEvaluatedKey' in response:
    scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']

    response = table.scan(**scan_params)
    data.extend(response['Items'])
    print(f'Exporting data... {len(data)} items')

csv_file = 'output.csv'
with open(csv_file, 'w', newline='') as file:
    fieldnames = data[0].keys()
    writer = csv.DictWriter(file, fieldnames)
    writer.writeheader()
    writer.writerows(data)