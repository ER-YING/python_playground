import gspread
import pandas as pd
import os
import io
import pickle
import base64
from googleapiclient.http import MediaIoBaseDownload
# --- Third-party libraries ---
from gspread_dataframe import get_as_dataframe
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
# ==============================================================================
# SCRIPT CONFIGURATION - - MODIFY THIS SECTION
# ==============================================================================
INTERNAL_IDS_TO_PROCESS = ['2344'] # Example: ['2049', '2050']
CLIENT_SECRETS_FILE = 'client_secret_445547975943-l4bvv3advto71sovc6rn5ndai1gmhib9.apps.googleusercontent.com.json'
PO_CHECK_SPREADSHEET_ID = '1DDb_Z08ET94wdmD6a93UPVgWQ73y0F2tQIlgHqMiFCc'
SERVICE_ACCOUNT_FILE = 'google_drive_sp_key.json'
INBOUND_SHIPMENT_TRACKER_TAB = 'Inbound Shipment Tracker'
SENDER_EMAIL = "ying@earthrated.com"

# --- Email Recipients for AN Automation ---
# US Recipients
BROKER_EMAIL = ["customs.phl@savinodelbene.com","theresa.biggin@savinodelbene.com"]  # <-- REPLACE with actual broker email
SEATRADE_EMAIL = ["stdlogistics@seatrade-usa.biz","azhang1@cosco-usa.com"] # <-- REPLACE with actual SeaTrade email
JD_EMAIL = ["jing.peng@jd.com", "us-b2b@jd.com", "steven.sun@jd.com"] 
CC_RECIPIENTS = ["ying@earthrated.com"]

# Canada Recipients
CANADA_BROKER_EMAIL = ["fernando@totalcustoms.ca"]
CANADA_CC_RECIPIENTS = ["operations@totalcustoms.ca", "ying@earthrated.com"]

# --- Signature Images ---
SIGNATURE_IMAGE_FILENAME = 'signature_logo.png'

# --- Automated Folder Search Configuration ---
OPERATIONS_SHARED_DRIVE_ID = "0AB9VXgYGzJL6Uk9PVA" 

# --- Supplier Name for Special Case ---
TIANJIN_YIYI_SUPPLIER_NAME = "Tianjin Yiyi Hygiene Products Co., Ltd."

# --- Location Configuration ---
US_LOCATIONS = ["Source NJ", "Source Montebello"]
CANADA_LOCATION = "Northland Goreway"

# --- Warehouse Delivery Addresses ---
WAREHOUSE_ADDRESSES = {
    "Source Montebello": "SOURCE LOGISTICS LOS ANGELES, LLC<br>812 UNION ST<br>MONTEBELLO, CA, US",
    "Source NJ": "2 Middlesex Avenue Suite A<br>Monroe Township, NJ 08831<br>United States"
}

# ==============================================================================
# AUTHENTICATION & CORE FUNCTIONS
# ==============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SHEETS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.send']
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def get_google_creds(token_name, scopes):
    """Handles user authentication for Google services."""
    token_path = os.path.join(SCRIPT_DIR, token_name)
    creds = None
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token: 
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, scopes)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'wb') as token: 
            pickle.dump(creds, token)
    return creds

def get_google_service_account_creds(credentials_file, scopes):
    """Handles service account authentication for Google Drive."""
    from google.oauth2 import service_account
    return service_account.Credentials.from_service_account_file(credentials_file, scopes=scopes)

def send_email_with_attachments(to, cc, subject, html_body, sender, attachments_list, images_to_embed):
    """Constructs and sends an email with attachments and embedded images."""
    creds = get_google_creds('token_gmail.pickle', GMAIL_SCOPES)
    service = build('gmail', 'v1', credentials=creds)
    message = MIMEMultipart('related')
    message['to'], message['from'], message['subject'] = ", ".join(to), sender, subject
    if cc: 
        message['cc'] = ", ".join(cc)
    
    msg_alternative = MIMEMultipart('alternative')
    message.attach(msg_alternative)
    msg_text = MIMEText(html_body, 'html')
    msg_alternative.attach(msg_text)
    
    for image_path in images_to_embed:
        image_cid = os.path.basename(image_path)
        try:
            with open(image_path, 'rb') as f:
                msg_image = MIMEImage(f.read())
                msg_image.add_header('Content-ID', f'<{image_cid}>')
                message.attach(msg_image)
        except FileNotFoundError: 
            print(f"Warning: Signature image '{image_path}' not found.")
            
    for name, data in attachments_list:
        part = MIMEApplication(data, Name=name)
        part['Content-Disposition'] = f'attachment; filename="{name}"'
        message.attach(part)
        
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    try:
        sent_message = service.users().messages().send(userId='me', body={'raw': raw_message}).execute()
        print(f"✅ Email '{subject}' with {len(attachments_list)} attachments sent successfully.")
    except Exception as error: 
        print(f"❌ An error occurred while sending email: {error}")

def find_shipment_subfolders(internal_id, operations_drive_id, service):
    """Finds the main shipment folder, then its 'Freight' and 'Paperwork' subfolders."""
    if not operations_drive_id or operations_drive_id == "YOUR_OPERATIONS_SHARED_DRIVE_ID_HERE":
        print("❌ FATAL ERROR: OPERATIONS_SHARED_DRIVE_ID is not set in the script configuration.")
        return None
    folder_ids = {'freight': None, 'paperwork': None}
    try:
        # Step 1: Find the main shipment folder (e.g., "INBSHIP2049 - NJ")
        print(f"Searching for main shipment folder containing 'INBSHIP{internal_id}'...")
        query = f"name contains 'INBSHIP{internal_id}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = service.files().list(q=query, corpora="drive", driveId=operations_drive_id,
                                       includeItemsFromAllDrives=True, supportsAllDrives=True,
                                       fields="files(id, name)").execute()
        shipment_folders = results.get('files', [])
        if not shipment_folders:
            print(f"   -> ❌ No main shipment folder found for Internal ID '{internal_id}'.")
            return None
        
        main_shipment_folder = shipment_folders[0]
        print(f"   -> Found main shipment folder: '{main_shipment_folder['name']}'")
        
        # Step 2: Find the 'Freight' and 'Paperwork' sub-folders
        for subfolder_name in ['Freight', 'Paperwork']:
            subfolder_query = f"'{main_shipment_folder['id']}' in parents and name='{subfolder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            subfolder_results = service.files().list(q=subfolder_query, supportsAllDrives=True,
                                                     includeItemsFromAllDrives=True, fields="files(id, name)").execute()
            found_folders = subfolder_results.get('files', [])
            if found_folders:
                folder_ids[subfolder_name.lower()] = found_folders[0]['id']
                print(f"   -> ✅ Found '{subfolder_name}' sub-folder.")
            else:
                print(f"   -> ❌ Could not find a '{subfolder_name}' sub-folder inside '{main_shipment_folder['name']}'.")
        return folder_ids
    except Exception as e:
        print(f"An error occurred while searching for Drive folders: {e}")
        return None

def download_drive_files(parent_folder_id, file_names_to_find, service):
    """Downloads specified files from a given parent folder ID on Google Drive."""
    downloaded_files = {}
    if not parent_folder_id: 
        return {}
    try:
        query = f"'{parent_folder_id}' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files_in_folder = results.get('files', [])
        for file_to_find in file_names_to_find:
            found_match = False
            for file in files_in_folder:
                if file['name'].lower() == file_to_find.lower():
                    print(f"   -> Downloading '{file['name']}'...")
                    request = service.files().get_media(fileId=file['id'], supportsAllDrives=True)
                    file_io = io.BytesIO()
                    downloader = MediaIoBaseDownload(file_io, request)
                    done = False
                    while not done: 
                        status, done = downloader.next_chunk()
                    file_io.seek(0)
                    downloaded_files[file['name']] = file_io.getvalue()
                    found_match = True
                    break
            if not found_match:
                print(f"   -> ⚠️  Warning: File '{file_to_find}' not found in folder ID {parent_folder_id}.")
    except Exception as e:
        print(f"   -> ❌ Error listing or downloading files: {e}")
        
    return downloaded_files

# ==============================================================================
# MAIN EXECUTION SCRIPT
# ==============================================================================
if __name__ == "__main__":
    print("Starting Arrival Notice (AN) Automation Script...")
    
    # Initialize Drive service once
    drive_creds = get_google_service_account_creds(SERVICE_ACCOUNT_FILE, DRIVE_SCOPES)
    drive_service = build('drive', 'v3', credentials=drive_creds)
    
    # Load data from Google Sheets
    try:
        sheets_creds = get_google_creds('token_sheets.pickle', SHEETS_SCOPES)
        client = gspread.authorize(sheets_creds)
        sheet = client.open_by_key(PO_CHECK_SPREADSHEET_ID)
        worksheet = sheet.worksheet(INBOUND_SHIPMENT_TRACKER_TAB)
        df = get_as_dataframe(worksheet, evaluate_formulas=True)
        df['Internal Id'] = df['Internal Id'].astype(str).str.strip().replace(r'\.0$', '', regex=True)
        print(f"Successfully loaded data from '{INBOUND_SHIPMENT_TRACKER_TAB}'.")
    except Exception as e:
        print(f"❌ FATAL ERROR: Could not load data from Google Sheet. {e}")
        exit()
    
    # Prepare signature images once
    images_to_embed = [os.path.join(SCRIPT_DIR, SIGNATURE_IMAGE_FILENAME)]
   
    signature_html = f"""
    <br><br>
    <div>
        <table cellpadding="0" cellspacing="0" border="0">
            <tr>
                <td valign="top" style="padding-right: 15px;">
                    <img src="cid:{SIGNATURE_IMAGE_FILENAME}" alt="Earth Rated Logo" width="120">
                </td>
                <td valign="top" style="color: #333333; border-left: 1px solid #dcdcdc; padding-left: 15px; font-size: 10pt;">
                    <b style="color: #00573c;">Ying Ji</b><br>
                    Import & Export Specialist<br>
                    Spécialiste, import et export<br><br>
                    1.888.354.2818<br>
                    earthrated.com<br><br>
                </td>
            </tr>
        </table>
        <p style="font-size: 10pt; color: #00573c; font-weight: bold; margin-top: 15px;">
            Proudly B Corp Certified<br>
            Entreprise certifiée B Corp
        </p>
    </div>
    """
    
    # Process each Internal ID
    for internal_id in INTERNAL_IDS_TO_PROCESS:
        print(f"\n{'='*25}\n--- Processing Internal ID: {internal_id} ---\n{'='*25}")
        
        shipment_df = df[df['Internal Id'] == internal_id].copy()
        if shipment_df.empty:
            print(f"Warning: Internal ID {internal_id} not found. Skipping.")
            continue
        
        first_row = shipment_df.iloc[0]
        supplier_name = str(first_row.get('Supplier Name', ''))
        container_number = str(first_row.get('Container#', 'N/A'))
        location = str(first_row.get('Location', 'N/A'))
        
        # Determine if this is a Canada or US shipment
        is_canada_shipment = (location == CANADA_LOCATION)
        is_us_shipment = location in US_LOCATIONS
        
        if not is_canada_shipment and not is_us_shipment:
            print(f"⚠️  Warning: Location '{location}' is neither US nor Canada. Skipping.")
            continue
        
        print(f"   -> Location: {location} ({'CANADA' if is_canada_shipment else 'US'} shipment)")
        
        # Extract data for dynamic email subjects
        po_subject_part = "+".join(shipment_df['PO#'].astype(str).unique())
        eta_date_str = 'N/A'
        if pd.notna(first_row.get('ETA')):
            try:
                eta_date = pd.to_datetime(first_row['ETA'])
                eta_date_str = eta_date.strftime('%-m/%-d')
            except Exception:
                print(f"   -> Warning: Could not format the ETA date '{first_row.get('ETA')}'")
        
        # Find the required 'Freight' and 'Paperwork' folder IDs
        folder_ids = find_shipment_subfolders(internal_id, OPERATIONS_SHARED_DRIVE_ID, drive_service)
        if not folder_ids or not folder_ids['freight'] or not folder_ids['paperwork']:
            print(f"❌ Critical folders missing in Drive for Internal ID {internal_id}. Cannot procee d. Skipping.")
            continue
        
        # ==============================================================================
        # CANADA CUSTOMS CLEARANCE LOGIC
        # ==============================================================================
        if is_canada_shipment:
            print("\n--- Processing CANADA Customs Clearance (Northland Goreway) ---")
            
            # Step 1: Define required documents (same as US broker)
            required_canada_docs = [
                f"INBSHIP{internal_id}AN.pdf",
                f"INBSHIP{internal_id}TLX.pdf"
            ]
            
            # Step 2: Add supplier-specific paperwork
            if supplier_name == TIANJIN_YIYI_SUPPLIER_NAME:
                print("   -> Special Case: 'Tianjin Yiyi' supplier detected. Requiring IV and PL paperwork.")
                required_canada_docs.extend([
                    f"INBSHIP{internal_id}PaperworkIV.pdf",
                    f"INBSHIP{internal_id}PaperworkPL.pdf"
                ])
            else:
                print("   -> Standard supplier. Requiring standard Paperwork.pdf.")
                required_canada_docs.append(f"INBSHIP{internal_id}Paperwork.pdf")
            
            # Step 3: Download documents
            canada_attachments_dict = {}
            freight_docs = [doc for doc in required_canada_docs if "Paperwork" not in doc]
            paperwork_docs = [doc for doc in required_canada_docs if "Paperwork" in doc]
            canada_attachments_dict.update(download_drive_files(folder_ids['freight'], freight_docs, drive_service))
            canada_attachments_dict.update(download_drive_files(folder_ids['paperwork'], paperwork_docs, drive_service))
            
            # Step 4: Validate all documents found
            found_docs_lower = [key.lower() for key in canada_attachments_dict]
            missing_docs = [doc for doc in required_canada_docs if doc.lower() not in found_docs_lower]
            
            if not missing_docs:
                print("   -> ✅ All required Canada customs documents found. Proceeding with email.")
                
                # Canada-specific email subject and body
                email_subject = f"EARTH RATED Arrival Notice - INBSHIP{internal_id} - {container_number}"
                html_body = f"""
                <html><body>
                <p>Hi Fernando,</p>
                <p>Could you please help to do the customs clearance for {internal_id} - {container_number}</p>
                <p>Please ignore the ocean freight invoice, and Earth Rated will make the payment to COSCO accordingly. The AN is on the second page.</p>
                <p>Thanks!</p>
                {signature_html}
                </body></html>"""
                
                canada_attachments_list = list(canada_attachments_dict.items())
                send_email_with_attachments(
                    to=CANADA_BROKER_EMAIL,
                    cc=CANADA_CC_RECIPIENTS,
                    subject=email_subject,
                    html_body=html_body,
                    sender=SENDER_EMAIL,
                    attachments_list=canada_attachments_list,
                    images_to_embed=images_to_embed
                )
                print("   -> ✅ Canada customs clearance email sent. Drayage skipped for Canada shipments.")
            else:
                print(f"❌ Canada customs email NOT sent for Internal ID {internal_id}. Missing documents: {', '.join(missing_docs)}")
            
            # Skip to next shipment (no drayage for Canada)
            continue
        
        # ==============================================================================
        # US CUSTOMS CLEARANCE LOGIC
        # ==============================================================================
        
        # --- 1. Broker AN Email (Customs Clearance) ---
        print("\n--- 1. Preparing US Broker AN Email ---")
        
        # Step 1: Define the base required documents
        required_broker_docs = [
            f"INBSHIP{internal_id}AN.pdf",
            f"INBSHIP{internal_id}TLX.pdf"
        ]
        
        # Step 2: Add supplier-specific paperwork documents
        if supplier_name == TIANJIN_YIYI_SUPPLIER_NAME:
            print("   -> Special Case: 'Tianjin Yiyi' supplier detected. Requiring IV and PL paperwork.")
            required_broker_docs.extend([
                f"INBSHIP{internal_id}PaperworkIV.pdf",
                f"INBSHIP{internal_id}PaperworkPL.pdf"
            ])
        else:
            print("   -> Standard supplier. Requiring standard Paperwork.pdf.")
            required_broker_docs.append(f"INBSHIP{internal_id}Paperwork.pdf")

        # Step 3: Download all required documents
        broker_attachments_dict = {}
        freight_docs = [doc for doc in required_broker_docs if "Paperwork" not in doc]
        paperwork_docs = [doc for doc in required_broker_docs if "Paperwork" in doc]
        broker_attachments_dict.update(download_drive_files(folder_ids['freight'], freight_docs, drive_service))
        broker_attachments_dict.update(download_drive_files(folder_ids['paperwork'], paperwork_docs, drive_service))

        # Step 4: Validate all documents found
        found_docs_lower = [key.lower() for key in broker_attachments_dict]
        missing_docs = [doc for doc in required_broker_docs if doc.lower() not in found_docs_lower]
        
        if not missing_docs:
            print("   -> ✅ All required broker documents found. Proceeding with email.")
            email_subject = f"EARTH RATED Arrival Notice - INBSHIP{internal_id} - {po_subject_part} ETA {location} {eta_date_str}"
            html_body = f"""
            <html><body>
            <p>Hi Theresa,</p>
            <p>Hope you have a great day :)</p>
            <p>Attached pls see AN & shipping doc for subject container.</p>
            <p>Let me know if any issues.</p>
            <p>Thank you!</p>
            {signature_html}
            </body></html>"""
            
            broker_attachments_list = list(broker_attachments_dict.items())
            send_email_with_attachments(
                to=BROKER_EMAIL, 
                cc=CC_RECIPIENTS, 
                subject=email_subject, 
                html_body=html_body, 
                sender=SENDER_EMAIL, 
                attachments_list=broker_attachments_list, 
                images_to_embed=images_to_embed
            )
        else:
            print(f"❌ Broker email NOT sent for Internal ID {internal_id}. Missing documents: {', '.join(missing_docs)}")

        # --- 2. Drayage Agent AN Email (Truck Booking) ---
        print("\n--- 2. Preparing US Drayage Agent AN Email ---")
        
        drayage_docs_to_find = [
            f"INBSHIP{internal_id}AN.pdf",
            f"INBSHIP{internal_id}TLX.pdf"
        ]
        drayage_attachments_dict = download_drive_files(folder_ids['freight'], drayage_docs_to_find, drive_service)
        
        # Check if the essential AN document was found
        an_doc_name = f"INBSHIP{internal_id}AN.pdf"
        if any(key.lower() == an_doc_name.lower() for key in drayage_attachments_dict):
            
            # Get the delivery address for this warehouse location
            delivery_address = WAREHOUSE_ADDRESSES.get(location, "")
            if not delivery_address:
                print(f"   -> ⚠️  Warning: No delivery address configured for location '{location}'.")
                delivery_address_html = ""
            else:
                delivery_address_html = f"<p><b>Delivery Address:</b><br>{delivery_address}</p>"
            
            # Interactive prompt for agent selection
            drayage_recipient = None
            drayage_body_content = ""
            while True:
                choice = input("Select drayage agent: (1 for SeaTrade, 2 for JD): ").strip()
                if choice == '1':
                    drayage_recipient = SEATRADE_EMAIL
                    drayage_body_content = f"""
                    <p>Hi Team,</p>
                    <p>Hope you have a great day :)</p>
                    <p>Attached pls see AN for subject container and kindly arrange delivery.</p>
                    {delivery_address_html}
                    <p><b>This is CY-DR service, we've paid drayage fee to COSCO.</b></p>
                    <p>Let me know if any issues.</p>"""
                    break
                elif choice == '2':
                    drayage_recipient = JD_EMAIL
                    drayage_body_content = f"""
                    <p>Hi Team,</p>
                    <p>Hope you have a great day :)</p>
                    <p>Attached pls see AN for subject container and kindly arrange delivery.</p>
                    {delivery_address_html}
                    <p>Let me know if any issues.</p>"""
                    break
                else:
                    print("Invalid input. Please enter 1 or 2.")
            
            # Combine body content with signature
            drayage_email_body = f"<html><body>{drayage_body_content}{signature_html}</body></html>"
            email_subject = f"Earth Rated AN - INBSHIP{internal_id} - {po_subject_part} - Container# {container_number} - ETA {location} {eta_date_str}"
            drayage_attachments_list = list(drayage_attachments_dict.items())
            
            send_email_with_attachments(
                to=drayage_recipient, 
                cc=CC_RECIPIENTS, 
                subject=email_subject, 
                html_body=drayage_email_body, 
                sender=SENDER_EMAIL, 
                attachments_list=drayage_attachments_list, 
                images_to_embed=images_to_embed
            )
        else:
            print(f"❌ Primary Arrival Notice document ('{an_doc_name}') not found. Cannot send drayage email. Skipping.")
    
    print("\n" + "="*50)
    print("✅ Arrival Notice Automation Script Completed!")
    print("="*50)