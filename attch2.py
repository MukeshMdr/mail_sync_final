import base64
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
# import html2text
from pdf2txt import convert_pdf_to_txt
import os
import base64
from googleapiclient.errors import HttpError
from pymongo import MongoClient


credentials_file_path = 'credentials.json'

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
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

medical_keywords = ["health", "medical", "doctor", "prescription", "treatment", "appointment", "diagnosis", "clinic"]

def download_attachments(service, msg_id, download_dir):
    try:
        # Initialize MongoDB connection
        client = MongoClient('mongodb+srv://mukesh:347qkpRJ2kpNrD1O@mydigirecords0.adupo8x.mongodb.net/')  # Update with your MongoDB connection string
        db = client['Mail_Sync']  # Update with your database name
        collection = db['attachments']
        
        message = service.users().messages().get(userId='me', id=msg_id).execute()
        from_address = message['payload']['headers'][0]['value']
        subject = next(header['value'] for header in message['payload']['headers'] if header['name'] == 'Subject')
        date = next(header['value'] for header in message['payload']['headers'] if header['name'] == 'Date')

        # print(f"Downloading attachments for message with ID: {msg_id}")

        if any(keyword in subject.lower() for keyword in medical_keywords):
            email_dir = os.path.join(download_dir, subject)

            if not os.path.exists(email_dir):
                os.makedirs(email_dir)

            for part in message['payload']['parts']:
                if 'body' in part:
                    if 'attachmentId' in part['body']:
                        attachment = service.users().messages().attachments().get(
                            userId='me', messageId=msg_id, id=part['body']['attachmentId']).execute()

                        file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))

                        # Create a unique filename or use a default name if the filename is empty
                        filename = part['filename'] or 'attachment.bin'
                        file_path = os.path.join(email_dir, filename)

                        with open(file_path, "wb") as attachment_file:
                            attachment_file.write(file_data)

                        # Convert PDF to text if the attachment is a PDF
                        if filename.lower().endswith('.pdf'):
                            pdf_path = file_path
                            txt_path = os.path.join(email_dir, os.path.splitext(filename)[0] + '.txt')
                            convert_pdf_to_txt(pdf_path, txt_path)

                            # Read the text content and store in MongoDB
                            with open(txt_path, 'r', encoding='utf-8') as txt_file:
                                attachment_content = txt_file.read()

                            # Store attachment in MongoDB
                            attachment_doc = {
                                'email_subject': subject,
                                'attachment_filename': filename,
                                'attachment_content': attachment_content
                            }
                            collection.insert_one(attachment_doc)

                        # print(f"Attachment downloaded: {filename}")
                        # file.write(f"Attachment: {filename}\n")

                    elif 'data' in part['body']:
                        file_data = base64.urlsafe_b64decode(part['body']['data'].encode('Latin'))

                        # Create a unique filename or use a default name if the filename is empty
                        filename = 'body.txt'
                        file_path = os.path.join(email_dir, filename)

                        with open(file_path, "wb") as attachment_file:
                            attachment_file.write(file_data)

                            # Read the text content and store in MongoDB
                            with open(file_path, 'r', encoding='utf-8') as txt_file:
                                attachment_content = txt_file.read()

                            # Store attachment in MongoDB
                            attachment_doc = {
                                'email_subject': subject,
                                'attachment_filename': filename,
                                'attachment_content': attachment_content
                            }
                            collection.insert_one(attachment_doc)

    except HttpError as error:
        print(f"An error occurred: {error}")
    finally:
        # Close MongoDB connection
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
