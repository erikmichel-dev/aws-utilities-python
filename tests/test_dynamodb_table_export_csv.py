import argparse
import pytest
from dynamodb_table_export_csv.main import export_dynamodb_to_csv
import boto3
import csv


@pytest.fixture
def dynamodb_localstack():
    return boto3.resource('dynamodb', endpoint_url="http://localhost:4566")


@pytest.fixture
def setup_table(dynamodb_localstack):
    table_data = [
        ["id", "name", "origin", "roast", "caffeine"],
        ["1", "Espresso", "Italy", "Dark", "High"],
        ["2", "Latte", "Italy", "Medium", "Medium"],
        ["3", "Cappuccino", "Italy", "Medium", "Medium"],
        ["4", "Americano", "United States", "Medium", "Low"],
        ["5", "Mocha", "Yemen", "Dark", "High"]
    ]

    try:
        table = dynamodb_localstack.create_table(
            TableName='test_table',
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'}
            ],
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        table.meta.client.get_waiter(
            'table_exists').wait(TableName='test_table')

        for row in table_data[1:]:
            id, name, origin, roast, caffeine = row
            table.put_item(Item={
                           'id': id, 'name': name, 'origin': origin, "roast": roast, "caffeine": caffeine})
    except Exception as e:
        print(f"Error creating table: {str(e)}")

    yield
    table.delete()


expected_content = [
    ["name", "caffeine", "id", "origin", "roast"],
    ["Espresso", "High", "1", "Italy", "Dark"],
    ["Americano", "Low", "4", "United States", "Medium"],
    ["Cappuccino", "Medium", "3", "Italy", "Medium"],
    ["Mocha", "High", "5", "Yemen", "Dark"],
    ["Latte", "Medium", "2", "Italy", "Medium"],
]


@pytest.mark.parametrize("expected_content", [expected_content])
def test_export_dynamodb_to_csv(setup_table, dynamodb_localstack, expected_content):
    args = argparse.Namespace(
        table_name="test_table", limit=None, rate_limit=None, attributes=None, profile=None)
    export_dynamodb_to_csv(args, dynamodb_localstack)

    actual_content = []
    with open('output.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            actual_content.append(row)
    assert actual_content == expected_content
