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
    logging.basicConfig(
        filename=log_filename,
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

def read_config(filename):
    config = configparser.ConfigParser()
    config.read(filename)
    return config

def query_sql_server(config):
    try:
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
    except Exception as e:
        logging.error(f"Error querying the SQL Server: {e}")
        raise

def write_to_csv(data, filename):
    try:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(data)
        logging.info(f"Results saved to {filename}")
    except Exception as e:
        logging.error(f"Error writing to CSV file: {e}")
        raise

def send_via_sftp(config, local_file):
    try:
        hostname = config['hostname']
        port = int(config['port'])
        username = config['username']
        password = config['password']
        remote_directory = config['remote_directory']

        logging.debug(f"SFTP Configuration: hostname={hostname}, port={port}, username={username}, remote_directory={remote_directory}")

        with paramiko.Transport((hostname, port)) as transport:
            transport.connect(username=username, password=password)
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                sftp.put(local_file, f"{remote_directory}/{os.path.basename(local_file)}")
                logging.info(f"Transferred {local_file} to {remote_directory}")
    except Exception as e:
        logging.error(f"Error during SFTP transfer: {e}")
        raise

def delete_csv_file(filename):
    try:
        os.remove(filename)
        logging.info(f"{filename} deleted.")
    except Exception as e:
        logging.error(f"Error deleting file {filename}: {e}")
        raise

def send_email(config, success=True):
    try:
        smtp_server = config['Email'].get('smtp_server')
        smtp_port = config['Email'].getint('smtp_port', fallback=587)
        sender_email = config['Email'].get('sender_email')
        sender_password = config['Email'].get('sender_password')
        receiver_emails = config['Email'].get('receiver_emails')

        if not (smtp_server and sender_email and sender_password and receiver_emails):
            logging.warning("Email configuration not found or incomplete. Skipping email notification.")
            return

        subject = 'CSV File Transmission Status'
        body = 'The CSV file has been successfully transmitted to the SFTP server.' if success else 'An error occurred while transmitting the CSV file to the SFTP server.'

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_emails
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_emails.split(','), msg.as_string())
        logging.info("Email notification sent.")
    except Exception as e:
        logging.error(f"Error sending email notification: {e}")

def main():
    setup_logger('transmission_log.log')
    logging.info("Starting CSV file transmission process.")

    try:
        config = read_config('conf.ini')

        result = query_sql_server(config)
        output_file = 'output.csv'
        write_to_csv(result, output_file)

        send_via_sftp(config['SFTP'], output_file)

        if config.getboolean('SFTP', 'delete_csv_after_transmit', fallback=False):
            delete_csv_file(output_file)

        send_email(config, success=True)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        send_email(config, success=False)

if __name__ == "__main__":
    main()
