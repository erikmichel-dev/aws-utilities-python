import boto3.session
import csv
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("table_name",
                    help="Table name to export")
parser.add_argument('--attributes', type=str,
                    help="A comma-separated list of attribute names to retrieve from the DynamoDB table. If specified, the scan will return only these attributes. If not provided, all attributes will be returned.")
parser.add_argument('--limit', type=int,
                    help="Maximum number of items to return from the scan operation. Default is 100.")
parser.add_argument('--profile', type=str,
                    help="AWS CLI profile to use for authentication. Defaults to 'default' if not specified.")
args = parser.parse_args()

try:
    session = boto3.session.Session(
        profile_name=args.profile if args.profile else 'default'
    )
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(args.table_name)
except Exception as e:
    print(f"Error initializing AWS session: {e}")
    exit(1)

scan_params: dict = {}
scan_params['Limit'] = args.limit if args.limit else 100

if args.attributes:
    attribute_list: list[str] = args.attributes.split(',')
    scan_params['Select'] = 'SPECIFIC_ATTRIBUTES'
    scan_params['ExpressionAttributeNames'] = {
        f"#{att}": att for att in attribute_list}
    scan_params['ProjectionExpression'] = ", ".join(
        f"#{att}" for att in attribute_list)
else:
    scan_params['Select'] = 'ALL_ATTRIBUTES'

try:
    response = table.scan(**scan_params)
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']

        response = table.scan(**scan_params)
        data.extend(response['Items'])
        print(f'Exporting data... {len(data)} items')
except Exception as e:
    print(f"Error scanning the table: {e}")
    exit(1)

try:
    csv_file = 'output.csv'
    with open(csv_file, 'w', newline='') as file:
        if data:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(file, fieldnames)
            writer.writeheader()
            writer.writerows(data)
            print(f"Data exported to {csv_file} successfully.")
        else:
            print("No data found on specified table")
except Exception as e:
    print(F"Error writing to CSV file: {e}")
