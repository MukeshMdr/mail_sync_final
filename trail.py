import base64
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from pdf2txt import convert_pdf_to_txt
from pymongo import MongoClient
from utils import medical_keywords, Mongo_client, Mongo_database, Mongo_collection
import shutil

credentials_file_path = '/home/mukesh/Tensorflow/Gmail_sync/credentials.json'

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def authenticate_gmail_api():
    creds = None
   
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json')
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def download_attachments(service, msg_id, download_dir):
    try:
        client = MongoClient(Mongo_client)
        db = client[Mongo_database] 
        collection = db[Mongo_collection]
        
        message = service.users().messages().get(userId='me', id=msg_id).execute()
        from_address = message['payload']['headers'][0]['value']
        subject = next(header['value'] for header in message['payload']['headers'] if header['name'] == 'Subject')
        date = next(header['value'] for header in message['payload']['headers'] if header['name'] == 'Date')

        if any(keyword in subject.lower() for keyword in medical_keywords):
            email_dir = os.path.join(download_dir, subject)

            if not os.path.exists(email_dir):
                os.makedirs(email_dir)

            txt_filename = os.path.join(email_dir, 'email_info.txt')

            with open(txt_filename, 'w', encoding='utf-8') as txt_file:
                txt_file.write(f"Email ID: {msg_id}\n")
                txt_file.write(f"From: {from_address}\n")
                txt_file.write(f"Date: {date}\n\n")

            for part in message['payload']['parts']:
                if 'body' in part:
                    if 'attachmentId' in part['body']:
                        attachment = service.users().messages().attachments().get(
                            userId='me', messageId=msg_id, id=part['body']['attachmentId']).execute()

                        file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                        filename = part['filename'] or 'attachment.bin'
                        file_path = os.path.join(email_dir, filename)

                        with open(file_path, "wb") as attachment_file:
                            attachment_file.write(file_data)

                        if filename.lower().endswith('.pdf'):
                            pdf_path = file_path
                            txt_path = os.path.join(email_dir, os.path.splitext(filename)[0] + '.txt')
                            convert_pdf_to_txt(pdf_path, txt_path)

                            with open(txt_path, 'r', encoding='utf-8') as pdf_txt_file:
                                attachment_content = pdf_txt_file.read()

                            attachment_info_filename = os.path.join(email_dir, f"{filename}_info.txt")
                            with open(attachment_info_filename, 'w', encoding='utf-8') as attachment_info_file:
                                attachment_info_file.write(f"Attachment Filename: {filename}\n")
                                attachment_info_file.write(attachment_content)

                    elif 'data' in part['body']:
                        file_data = base64.urlsafe_b64decode(part['body']['data'].encode('Latin'))
                        filename = 'body.txt'
                        file_path = os.path.join(email_dir, filename)

                        with open(file_path, "wb") as attachment_file:
                            attachment_file.write(file_data)

                        with open(file_path, 'r', encoding='utf-8') as body_txt_file:
                            attachment_content = body_txt_file.read()

                        body_info_filename = os.path.join(email_dir, 'body_info.txt')
                        with open(body_info_filename, 'w', encoding='utf-8') as body_info_file:
                            body_info_file.write(f"Body Filename: {filename}\n")
                            body_info_file.write(attachment_content)

            mail_info = {
                'email_subject': subject,
                'from_address': from_address,
                'date': date,
                'txt_filename': txt_filename
            }

            collection.insert_one(mail_info)

    except HttpError as error:
        print(f"An error occurred: {error}")
    finally:
        client.close()

def main():
    creds = authenticate_gmail_api()
    
    service = build('gmail', 'v1', credentials=creds)

    try:
        results = service.users().messages().list(userId='me', labelIds=['INBOX']).execute()
        messages = results.get('messages', [])

        download_dir = 'Med_email'

        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        for message in messages:
            download_attachments(service, message['id'], download_dir)

    except HttpError as error:
        print(f"An error occurred: {error}")

if __name__ == '__main__':
    main()
