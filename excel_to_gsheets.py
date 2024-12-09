import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import os.path
import pickle
import time

# Google Sheets API scope
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def copy_csv_to_gsheets(csv_file_path, sheet_id, sheet_name):
    # Read CSV file
    df = pd.read_csv(csv_file_path)
    
    # Convert the 'Completed' column from YYYYMMDD to MM/DD/YYYY format
    df['Completed'] = pd.to_datetime(df['Completed'], format='%Y%m%d').dt.strftime('%m/%d/%Y')
    
    # Clean the data
    df = df.replace({pd.NA: None, pd.NaT: None})
    df = df.where(pd.notnull(df), None)
    
    # Get the range A3:CA
    data = df.iloc[2:, :79].values.tolist()
    
    # Clean any problematic values in the nested lists
    data = [[None if pd.isna(cell) else cell for cell in row] for row in data]
    
    # Set up Google Sheets credentials using OAuth 2.0
    creds = None
    # The file token.pickle stores the user's access and refresh tokens
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If credentials are not valid or don't exist, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    # Build Google Sheets service
    service = build('sheets', 'v4', credentials=creds)
    
    # Clear the existing content first
    clear_range = 'VIP Results!A6:CA10000'
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=clear_range
        ).execute()
        print("Successfully cleared previous data")
    except Exception as e:
        print(f"Error clearing data: {str(e)}")
        return False
    
    # Prepare the values for update
    body = {
        'values': data
    }
    
    # Update Google Sheet
    range_name = 'VIP Results!A6:CA'
    try:
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        print(f"Cells updated: {result.get('updatedCells')}")
        
        # After writing the data, apply time format to the Time column
        format_request = {
            'requests': [{
                'repeatCell': {
                    'range': {
                        'sheetId': 0,  # Assuming first sheet
                        'startColumnIndex': 4,  # 0-based index for column E
                        'endColumnIndex': 5,
                        'startRowIndex': 5  # Starting from row 6
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'TIME',
                                'pattern': 'h:mm am/pm'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            }]
        }
        
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body=format_request
        ).execute()
        
        return True
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False

def clear_sheet_contents(service, sheet_id):
    clear_range = 'VIP Results!A6:CA10000'
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=clear_range
        ).execute()
        print("Successfully cleared previous data")
        return True
    except Exception as e:
        print(f"Error clearing data: {str(e)}")
        return False

def get_credentials():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return creds

def update_red_list(service, sheet_id):
    try:
        # Get the date range from RYG Dashboard sheet
        date_range = 'RYG Dashboard!B7:B11'
        date_result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=date_range
        ).execute()
        dates = date_result.get('values', [])
        
        # Convert dates and find min/max
        all_dates = []
        for date_row in dates:
            if date_row and date_row[0]:
                try:
                    date = pd.to_datetime(date_row[0], format='%m/%d/%Y')
                    all_dates.append(date)
                except (ValueError, TypeError):
                    continue
                    
        if all_dates:
            min_date = min(all_dates).strftime('%m/%d/%Y')
            max_date = max(all_dates).strftime('%m/%d/%Y')
            date_range_str = f"{min_date} - {max_date}"
        else:
            date_range_str = "Date range not found"

        # Get the data from RYG Dashboard (now including column J for commitments)
        range_name = 'RYG Dashboard!C6:J1000'  # Extended to include column J
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        values = result.get('values', [])

        # Process the data for both visits and commitments
        red_list_data = []
        for row in values:
            # Check if row has all required data (Rep & Brand, Region)
            if len(row) >= 4 and row[0] and row[1]:  # Ensure columns C and D have values
                rep_brand = row[0]  # Column C - Unique Rep & Brand
                region = row[1]     # Column D - Region
                
                # Initialize values
                visit_rep = ""
                visit_region = ""
                commitment_rep = ""
                commitment_region = ""
                
                try:
                    visits = int(row[3])  # Column F - Total Unique Visits
                    commitments = int(row[7]) if len(row) > 7 else 0  # Column J - Commitments
                    
                    # Check visits
                    if visits < 32:
                        visit_rep = rep_brand
                        visit_region = region
                        print(f"Adding to Visit Red List: {rep_brand} from {region} - {visits} visits")
                    
                    # Check commitments
                    if commitments < 7:
                        commitment_rep = rep_brand
                        commitment_region = region
                        print(f"Adding to Commitment Red List: {rep_brand} from {region} - {commitments} commitments")
                    
                    # Only add to red_list_data if either condition is met
                    if visits < 32 or commitments < 7:
                        red_list_data.append([
                            date_range_str,      # Column A - Date Range
                            visit_rep,           # Column B - Rep & Brand (for visits)
                            visit_region,        # Column C - Region (for visits)
                            commitment_rep,      # Column D - Rep & Brand (for commitments)
                            commitment_region    # Column E - Region (for commitments)
                        ])
                except (ValueError, TypeError):
                    continue

        if red_list_data:
            print("\nSummary of entries to be added:")
            for date_range, rep, region, comm_rep, comm_region in red_list_data:
                print(f"- {region}: {rep}" + (f" (also in commitment list)" if comm_rep else ""))
            
            # Find the first empty row in the Red List sheet
            red_list_range = 'Red List!A:E'  # Updated to include all columns
            result = service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=red_list_range
            ).execute()
            current_values = result.get('values', [])
            next_row = len(current_values) + 1

            # Append to Red List
            body = {
                'values': red_list_data
            }
            append_range = f'Red List!A{next_row}:E{next_row + len(red_list_data) - 1}'  # Updated range
            service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=append_range,
                valueInputOption='RAW',
                body=body
            ).execute()
            print(f"\nSuccessfully appended {len(red_list_data)} entries to Red List starting at row {next_row}")
        else:
            print("No entries found for either list")
        
        return True
    except Exception as e:
        print(f"Error updating Red List: {str(e)}")
        return False

if __name__ == "__main__":
    # File paths and sheet details
    csv_file = "csv/Results-KARMA-Applications-VIP-SRS-20241209.csv"
    google_sheet_id = "1uvn_ffYHWXrngi6_REGYZg6mqXmWIR6L8fKwhrgSZYg"
    target_sheet_name = "VIP Results"
    
    while True:
        choice = input("""
Choose an action:
1. Clear sheet contents only (A6:CA10000)
2. Run full script (clear and update)
3. Exit
Enter your choice (1, 2, or 3): """).strip()
        
        if choice not in ['1', '2', '3']:
            print("Invalid choice. Please enter 1, 2, or 3.")
            continue
            
        if choice == '3':
            print("Exiting script...")
            break
            
        # Set up credentials and service for either option
        creds = get_credentials()  # You'll need to extract your credential setup into a function
        service = build('sheets', 'v4', credentials=creds)
            
        if choice == '1':
            clear_sheet_contents(service, google_sheet_id)
            break
        elif choice == '2':
            copy_csv_to_gsheets(csv_file, google_sheet_id, target_sheet_name)
            update_red_list(service, google_sheet_id)
            break