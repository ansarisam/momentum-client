import configparser
import pyodbc
import csv
import paramiko
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import os

def setup_logger(log_filename):
    logging.basicConfig(filename=log_filename, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def read_config(filename):
    config = configparser.ConfigParser()
    config.read(filename)
    return config

def query_sql_server(config):
    query = config['DBServer']['query']
    conn_str = (
        f"DRIVER={config['DBServer']['driver']};"
        f"SERVER={config['DBServer']['host']};"
        f"DATABASE={config['DBServer']['database']};"
        f"UID={config['DBServer']['username']};"
        f"PWD={config['DBServer']['password']}"
    )
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows

def write_to_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)

def send_via_sftp(config, local_file):
    hostname = config['SFTP']['hostname']
    port = int(config['SFTP']['port'])
    username = config['SFTP']['username']
    password = config['SFTP']['password']
    remote_directory = config['SFTP']['remote_directory']

    with paramiko.Transport((hostname, port)) as transport:
        transport.connect(username=username, password=password)
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(local_file, f"{remote_directory}/{os.path.basename(local_file)}")

def delete_csv_file(filename):
    os.remove(filename)
    logging.info(f"{filename} deleted.")

def send_email(config, success=True):
    smtp_server = config['Email'].get('smtp_server')
    smtp_port = config['Email'].getint('smtp_port', fallback=587)
    sender_email = config['Email'].get('sender_email')
    sender_password = config['Email'].get('sender_password')
    receiver_emails = config['Email'].get('receiver_emails')

    if not (smtp_server and sender_email and sender_password and receiver_emails):
        logging.warning("Email configuration not found or incomplete. Skipping email notification.")
        return

    subject = 'CSV File Transmission Status'
    if success:
        body = 'The CSV file has been successfully transmitted to the SFTP server.'
    else:
        body = 'An error occurred while transmitting the CSV file to the SFTP server.'

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_emails
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_emails.split(','), msg.as_string())
        logging.info("Email notification sent.")
    except Exception as e:
        logging.error(f"Error sending email notification: {str(e)}")

def main():
    setup_logger('transmission_log.log')
    logging.info("Starting CSV file transmission process.")

    config = read_config('conf.ini')
    try:
        result = query_sql_server(config['DBServer'])
        output_file = input("Enter the filename to save the results (e.g., output.csv): ")
        write_to_csv(result, output_file)
        logging.info(f"Results saved to {output_file}")

        sftp_config = config['SFTP']
        send_via_sftp(sftp_config, output_file)
        logging.info(f"{output_file} sent to SFTP server.")

        if config.getboolean('SFTP', 'delete_csv_after_transmit', fallback=False):
            delete_csv_file(output_file)

        send_email(config, success=True)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        send_email(config, success=False)

if __name__ == "__main__":
    main()

