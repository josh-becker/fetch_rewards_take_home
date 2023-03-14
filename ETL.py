import boto3
from cryptography.fernet import Fernet
import json
import psycopg2
from datetime import date

def get_sqs_messages(url):
    """Function to grab all SQS messages from the queue and format them for storage"""
    key = Fernet.generate_key()
    fernet = Fernet(key)
    sqs_client = boto3.client("sqs", endpoint_url=url)
    messages = [] #empty list of unformatted messages
    while True:
        response = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=10) #documentation advises not to go above 10 messages in a single request
        try:
            current_batch = response["Messages"]
        except:
            break #no more messages to read
        for y in current_batch:
            messages.append(y)
    data = [] #empty list of formatted messages
    current_date = date.today() #assume all of these messages are from today
    for i in messages:
        body = i["Body"]
        json_body = json.loads(body)
        try:
            user_id = json_body["user_id"]
            device_id = json_body["device_id"]
            ip = json_body["ip"]
            device_type = json_body["device_type"]
            locale = json_body["locale"]
            app_version = json_body["app_version"].split(".")[0]
            encrypted_device = fernet.encrypt(device_id.encode())
            encrypted_ip = fernet.encrypt(ip.encode())
            string_device_masked = encrypted_device.decode()
            string_ip_masked = encrypted_ip.decode()
            message = {'user_id': user_id, 'device_type': device_type, 'masked_ip': string_ip_masked, 'masked_device_id': string_device_masked,
                'locale': locale, 'app_version': app_version, 'create_date': current_date}
            data.append(message)
        except:
            pass #do nothing, message does not fit format
    return data

def write_to_postgres(data, host, port, username, password, database):
    """Function to write all formatted messages to the postgres database"""
    try:
        connection = psycopg2.connect(user=username, password=password, host=host, port=port, database=database)
        cursor = connection.cursor()
        postgres_insert_query = """ INSERT INTO user_logins (USER_ID, DEVICE_TYPE, MASKED_IP, MASKED_DEVICE_ID, LOCALE, APP_VERSION, CREATE_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s) """
        for i in data:
            record_to_insert = (i["user_id"], i["device_type"], i["masked_ip"], i["masked_device_id"], i["locale"],
                i["app_version"], i["create_date"])
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
        print("All logins successfully written to postgres")
    except:
        print("Issue encountered while writing to postgres")
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Connection closed")

def main():
    """Main function where constants are defined and the get_sqs_messages and write_to_postgres functions are called"""
    sqs_url = "http://localhost:4566/000000000000/login-queue"
    postgres_host = "127.0.0.1"
    postgres_port = "5432"
    postgres_username = "postgres"
    postgres_password = "postgres"
    postgres_database = "postgres"
    data = get_sqs_messages(sqs_url)
    print("Read", len(data), "correctly formatted records from SQS")
    write_to_postgres(data, postgres_host, postgres_port, postgres_username, postgres_password, postgres_database)


if __name__=="__main__":
    main()