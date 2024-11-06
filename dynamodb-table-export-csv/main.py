import boto3.session
import csv
import argparse
from typing import Any
from argparse import Namespace


def parse_arguments() -> Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("table_name",
                        help="Table name to export")
    parser.add_argument('--attributes', type=str,
                        help="A comma-separated list of attribute names to retrieve from the DynamoDB table. If specified, the scan will return only these attributes. If not provided, all attributes will be returned.")
    parser.add_argument('--limit', type=int,
                        help="Maximum number of items to return from the scan operation. Default is 100.")
    parser.add_argument('--profile', type=str,
                        help="AWS CLI profile to use for authentication. Defaults to 'default' if not specified.")
    return parser.parse_args()


def export_dynamodb_to_csv(args: Namespace) -> None:
    try:
        session = boto3.session.Session(
            profile_name=args.profile if args.profile else 'default'
        )
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(args.table_name)
    except Exception as e:
        print(f"Error initializing AWS session: {e}")
        exit(1)

    scan_params: dict[str, Any] = {}
    scan_params['Limit'] = args.limit if args.limit else 100

    if args.attributes:
        attribute_list = args.attributes.split(',')
        scan_params['Select'] = 'SPECIFIC_ATTRIBUTES'
        scan_params['ExpressionAttributeNames'] = {
            f"#{att}": att for att in attribute_list}
        scan_params['ProjectionExpression'] = ", ".join(
            f"#{att}" for att in attribute_list)
    else:
        scan_params['Select'] = 'ALL_ATTRIBUTES'

    try:
        exported_items: list[dict[str, Any]] = []
        while True:
            response = table.scan(**scan_params)
            if not response['Items']:
                break
            exported_items.extend(response['Items'])
            print(f'Exporting data... {len(exported_items)} items')
            if 'LastEvaluatedKey' not in response:
                break
            scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']

    except Exception as e:
        print(f"Error scanning the table: {e}")
        exit(1)

    try:
        csv_file = 'output.csv'
        with open(csv_file, 'w', newline='') as file:
            if exported_items:
                fieldnames = exported_items[0].keys()
                writer = csv.DictWriter(file, fieldnames)
                writer.writeheader()
                writer.writerows(exported_items)
                print(f"Data exported to {csv_file} successfully.")
            else:
                print("No data found on specified table")
    except Exception as e:
        print(F"Error writing to CSV file: {e}")


def main():
    export_dynamodb_to_csv(
        parse_arguments()
    )


if __name__ == "__main__":
    main()
