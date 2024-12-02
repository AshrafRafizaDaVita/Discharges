import pandas as pd
import numpy as np
import os
from config import DATA_FOLDER
from carryforward_deaths import carryForward_death
from regionList import region_list

# get latest file function (CSV)
def getLatesFile(directory_path):

    # Get a list of all CSV files in the directory
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

    if not csv_files:
        print("No CSV files found in the specified directory.")
    else:
        # Find the latest CSV file based on modification time
        # latest_csv = max(csv_files, key=lambda x: os.path.getmtime(os.path.join(directory_path, x)))
        latest_csv = max(csv_files, key=lambda x: os.path.getmtime(os.path.join(directory_path, x)))
        print(latest_csv)

        # Full path to the latest CSV file
        latest_csv_path = os.path.join(directory_path, latest_csv)

    return latest_csv_path

# Read Patient Attrition
def readAttrition():
    folderpath = os.path.join(DATA_FOLDER, 'Patient Attrition')

    latest_csv_path = getLatesFile(folderpath)
    df = pd.read_csv(latest_csv_path, skiprows = 3)

    df = df[df['Discharge Type'] == 'Death']

    df = df[[
        'MR No.',
        'Physical Discharge Remarks'
    ]]

    return df

# Columns to drop
col_to_drop = [
    'Duplicate MR No.',
    'Discharge Type',
    'Date of Birth',
    'Gender',
    'National/Passport ID Type',
    'National/Passport ID [NRIC: ******-**-****][Police ID: RF/******][Army ID: T*******]',
    'First Dialysis Date in Davita', 
    'First Dialysis Date(FDODD)',
    'Virology Status Date', 
    'Virology Status', 
    'Blood Group',
    'PDPA Consent (Yes/No)', 
    'Patient Sources',
    'Patient Referral Source Hospital', 
    'Religion', 
    'Address',
]


# Read Mortality
def readMortality():
    # READ DEATH REPORT
    folderpath = os.path.join(DATA_FOLDER, 'Death')
    latest_csv_path = getLatesFile(folderpath)

    df = pd.read_csv(latest_csv_path, skiprows = 2)
    df['Region'] = df['Primary Center'].map(region_list)

    # Add month column
    df['Death Date'] = pd.to_datetime(df['Death Date'], dayfirst=True)

    # Extract month and year from 'Death Date'
    df['Month'] = df['Death Date'].dt.strftime('%Y-%m')

    df['Week'] = df['Death Date'].dt.isocalendar().week

    # Replace Others (Please write in Discharge Remarks box) to Others
    df['Death Reason'] = df['Death Reason'].replace({'Others (Please write in Discharge Remarks box)':'Others'})

    for key, value in carryForward_death.items():
        if key not in df['MR No.'].unique():
            print(f"Carry forward death not in data: {key} {value}")

    # Apply new month for carry forward deaths
    df['Month'] = df.apply(
        lambda row: carryForward_death[row['MR No.']] if row['MR No.'] in carryForward_death else row['Month'],
        axis=1
    )

    # Sort the df
    df = df.sort_values(by='Week', ascending=False)

    # READ PATIENT ATTRITION
    attrition = readAttrition()

    # MERGE
    df = pd.merge(df, attrition, on='MR No.', how='left')

    # Relocate Remarks column
    df.insert(7, "Physical Discharge Remarks", df.pop("Physical Discharge Remarks"))

    # Drop columns
    df = df.drop(col_to_drop, axis=1)

    return df