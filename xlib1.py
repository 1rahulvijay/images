import os
import pandas as pd
from exchangelib import (
    Credentials, Account, DELEGATE, FileAttachment, Message, Folder
)
from datetime import datetime

# ======================================
# 1. CONFIGURATION
# ======================================

USERNAME = "your_email@domain.com"
PASSWORD = "your_password"

MAPPING_FILE = "mapping_table.xlsx"   # Must contain: Subject | Filename | SavePath
LOG_FILE = "download_log.csv"

DELETE_FOLDER_NAME = "Deleted Items"  # Could also use "Archive" or custom folder

# ======================================
# 2. UTILS
# ======================================

def load_mapping(mapping_file):
    """Load mapping from Excel/CSV into dictionary"""
    if mapping_file.endswith(".xlsx") or mapping_file.endswith(".xls"):
        df = pd.read_excel(mapping_file)
    else:
        df = pd.read_csv(mapping_file)

    mapping = {}
    for _, row in df.iterrows():
        subject = str(row['Subject']).strip().lower()
        mapping[subject] = {
            "filename": str(row['Filename']).strip(),
            "save_path": str(row['SavePath']).strip()
        }
    return mapping


def log_download(mail_id, subject, filename, save_path):
    """Log successful downloads into a CSV log file"""
    log_entry = {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "MailID": str(mail_id),
        "Subject": subject,
        "Filename": filename,
        "SavedTo": save_path
    }
    if not os.path.exists(LOG_FILE):
        pd.DataFrame([log_entry]).to_csv(LOG_FILE, index=False)
    else:
        df = pd.DataFrame([log_entry])
        df.to_csv(LOG_FILE, mode="a", header=False, index=False)


def connect_outlook():
    """Connect to Outlook account"""
    creds = Credentials(username=USERNAME, password=PASSWORD)
    return Account(
        primary_smtp_address=USERNAME,
        credentials=creds,
        autodiscover=True,
        access_type=DELEGATE
    )

# ======================================
# 3. PROCESS INBOX
# ======================================

def download_and_move(account, mapping):
    inbox = account.inbox
    delete_folder = account.trash  # Default "Deleted Items"
    # If you want a custom folder, uncomment:
    # delete_folder = account.root / 'Top of Information Store' / DELETE_FOLDER_NAME

    for item in inbox.filter(is_read=False).order_by('-datetime_received')[:200]:
        if isinstance(item, Message):
            subject = item.subject.strip().lower() if item.subject else ""

            # Find mapping by subject (partial match allowed)
            matched_key = None
            for key in mapping.keys():
                if key in subject:
                    matched_key = key
                    break

            if not matched_key:
                continue

            map_entry = mapping[matched_key]
            expected_filename = map_entry["filename"]
            save_path = map_entry["save_path"]
            os.makedirs(save_path, exist_ok=True)

            downloaded = False
            for attachment in item.attachments:
                if isinstance(attachment, FileAttachment):
                    if attachment.name.lower().endswith(('.xlsx', '.xls', '.csv')):

                        # check filename match if specified
                        if expected_filename and expected_filename.lower() != attachment.name.lower():
                            continue  

                        file_path = os.path.join(save_path, attachment.name)

                        # Skip if already exists
                        if os.path.exists(file_path):
                            print(f"⚠️ File already exists, skipping: {file_path}")
                            continue

                        with open(file_path, 'wb') as f:
                            f.write(attachment.content)

                        print(f"✅ Saved {attachment.name} to {file_path}")
                        log_download(item.id, subject, attachment.name, file_path)
                        downloaded = True

            if downloaded:
                # Mark as read + move to Deleted folder
                item.is_read = True
                item.save()
                item.move(delete_folder)
                print(f"🗑️ Moved mail '{subject}' to {DELETE_FOLDER_NAME}")

    print("✅ Inbox processing completed.")


# ======================================
# MAIN
# ======================================
if __name__ == "__main__":
    mapping = load_mapping(MAPPING_FILE)
    account = connect_outlook()
    download_and_move(account, mapping)
