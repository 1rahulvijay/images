import os
import logging
from datetime import datetime, timedelta
import json
from exchangelib import Credentials, Account, DELEGATE, FileAttachment, EWSDateTime
from exchangelib.util import to_timezone_aware_datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load configuration from a JSON file (create this file with your settings)
CONFIG_FILE = 'config.json'

def load_config():
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Configuration file '{CONFIG_FILE}' not found. Please create it with your settings.")
    
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
    
    required_keys = ['username', 'password', 'primary_smtp', 'mapping_table', 'days_to_look_back']
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required key '{key}' in configuration file.")
    
    return config

# Main function
def main():
    try:
        config = load_config()
        
        # Credentials (in production, use secure methods like key vaults)
        credentials = Credentials(username=config['username'], password=config['password'])
        account = Account(
            primary_smtp_address=config['primary_smtp'],
            credentials=credentials,
            autodiscover=True,
            access_type=DELEGATE
        )
        
        mapping_table = config['mapping_table']
        days_to_look_back = config['days_to_look_back']
        
        # Calculate date filter: emails from the last N days
        since_date = to_timezone_aware_datetime(datetime.now() - timedelta(days=days_to_look_back), account.default_timezone)
        
        for mapping in mapping_table:
            subject = mapping.get('subject')
            filename = mapping.get('filename').lower()  # For case-insensitive matching
            save_folder = mapping.get('save_folder')
            file_types = mapping.get('file_types', ['xlsx', 'csv'])  # Default to excel and csv
            
            if not all([subject, filename, save_folder]):
                logger.warning(f"Skipping incomplete mapping: {mapping}")
                continue
            
            logger.info(f"Processing mapping for subject containing: '{subject}'")
            
            # Filter emails: containing subject (case-insensitive), received since date, has attachments
            items = account.inbox.filter(
                subject__icontains=subject,
                datetime_received__gte=since_date,
                has_attachments=True
            ).order_by('-datetime_received')
            
            if not items:
                logger.info(f"No matching emails found for subject containing: '{subject}'")
                continue
            
            for item in items:
                logger.info(f"Processing email: '{item.subject}' received at {item.datetime_received}")
                
                saved = False
                for attachment in item.attachments:
                    if isinstance(attachment, FileAttachment):
                        att_name_lower = attachment.name.lower()
                        if att_name_lower == filename or any(att_name_lower.endswith(f".{ext}") for ext in file_types if filename == '*'):
                            # Create save folder if it doesn't exist
                            os.makedirs(save_folder, exist_ok=True)
                            
                            local_path = os.path.join(save_folder, attachment.name)
                            
                            # Check if file already exists to avoid duplicates
                            if os.path.exists(local_path):
                                logger.warning(f"File '{local_path}' already exists. Skipping.")
                                continue
                            
                            # Save the attachment
                            with open(local_path, 'wb') as f:
                                f.write(attachment.content)
                            
                            logger.info(f"Saved attachment '{attachment.name}' from email '{item.subject}' to '{local_path}'")
                            saved = True
                
                if saved:
                    # Optional: Mark email as read and/or move to a processed folder
                    item.is_read = True
                    item.save(update_fields=['is_read'])
                    # To move: processed_folder = account.inbox / 'Processed'
                    # item.move(processed_folder)
        
        logger.info("Processing complete.")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()


// Example config.json file content (create this file separately)
{
    "username": "your_username@outlook.com",
    "password": "your_password",
    "primary_smtp": "your_username@outlook.com",
    "days_to_look_back": 7,
    "mapping_table": [
        {
            "subject": "Example Subject 1",
            "filename": "data.xlsx",
            "save_folder": "/path/to/folder1",
            "file_types": ["xlsx"]
        },
        {
            "subject": "Example Subject 2",
            "filename": "info.csv",
            "save_folder": "/path/to/folder2",
            "file_types": ["csv"]
        },
        {
            "subject": "Any Attachment",
            "filename": "*",  // Use * for any filename matching file_types
            "save_folder": "/path/to/folder3",
            "file_types": ["xlsx", "csv"]
        }
    ]
}
