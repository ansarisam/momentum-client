import configparser
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

def delete_csv_file(filename):
    try:
        os.remove(filename)
        logging.info(f"{filename} deleted.")
    except Exception as e:
        logging.error(f"Error deleting file {filename}: {e}")

def send_via_sftp(config):
    try:
        hostname = config['hostname']
        port = int(config['port'])
        username = config['username']
        password = config['password']
        remote_directory = config['remote_directory']
        local_dir = config['local_dir']
        delete_after_transmit = config.getboolean('delete_csv_after_transmit', fallback=False)

        logging.debug(f"SFTP Configuration: hostname={hostname}, port={port}, username={username}, remote_directory={remote_directory}, local_dir={local_dir}")

        with paramiko.Transport((hostname, port)) as transport:
            transport.connect(username=username, password=password)
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                for local_file in os.listdir(local_dir):
                    local_file_path = os.path.join(local_dir, local_file)
                    if os.path.isfile(local_file_path):
                        remote_file_path = f"{remote_directory}/{local_file}"
                        sftp.put(local_file_path, remote_file_path)
                        logging.info(f"Transferred {local_file_path} to {remote_file_path}")
                        if delete_after_transmit:
                            delete_csv_file(local_file_path)
    except Exception as e:
        logging.error(f"An error occurred during SFTP transfer: {e}")
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
    logging.info("Starting file transmission process.")

    try:
        config = read_config('conf.ini')
        send_via_sftp(config['SFTP'])
        logging.info("All files from the input dir sent to SFTP server.")
        send_email(config, success=True)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        send_email(config, success=False)

if __name__ == "__main__":
    main()
