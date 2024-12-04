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

# Columns to drop in Death Report
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

    return df

# Read All Sponsor report to get patient sponsor
def readSponsor():
    folderpath = os.path.join(DATA_FOLDER, 'All Sponsor')
    latest_csv_path = getLatesFile(folderpath)

    df = pd.read_csv(latest_csv_path, skiprows = 1)

    df = df[[
        'MR No.',
        'Sponsor Name1',
        'Sponsor Name2',
        'Sponsor Name3',
        'Sponsor Name4',
        'Sponsor Name5',
        'Status',
        'Item (Infusions)',
        'Item (Haemodialysis)'
    ]]

    # Filter Status as Active
    df = df[df['Status'] == 'Active']

    # Item (Infusions) or Item (Haemodialysis) is HAEMODIALYSIS
    df = df[(df['Item (Infusions)'] == 'HAEMODIALYSIS') | (df['Item (Haemodialysis)'] == 'HAEMODIALYSIS')]

    # Create a prioritized column where null values in Sponsor Name1 are replaced by non-null values in the subsequent columns
    df['Sponsor Name1'] = df[['Sponsor Name1', 'Sponsor Name2', 'Sponsor Name3', 'Sponsor Name4', 'Sponsor Name5']].bfill(axis=1)['Sponsor Name1']

    # Drop columns
    df = df.drop([
        'Sponsor Name2', 
        'Sponsor Name3', 
        'Sponsor Name4', 
        'Sponsor Name5',
        'Item (Infusions)',
        'Item (Haemodialysis)',
        'Status'
    ], axis=1)

    # Rename col
    df = df.rename({'Sponsor Name1' : 'Sponsor'}, axis=1)

    # df = df.drop(df_col_drop, axis=1)

    return df

# Read Death Category
def readDeath_category():
    folderpath = os.path.join(DATA_FOLDER, 'Death Category') 
    file = [file for file in os.listdir(folderpath)][0]

    df = pd.read_excel(os.path.join(folderpath, file))
    df = df[['MR No.', 'Death Category']]

    return df

# Generate Death Data
def generate_deathData():
    death = readMortality()
    attrition = readAttrition()
    sponsor = readSponsor()
    death_category = readDeath_category()

    # MERGE
    df = pd.merge(death, attrition, on='MR No.', how='left') # Death Report n Patient Attrition (Discharge Remarks)
    df = pd.merge(df, sponsor, on='MR No.', how='left') # Merged n Sponsor
    df = pd.merge(df, death_category, on='MR No.', how='left') # Merged n Death Category

    # Relocate column
    df.insert(0, "Region", df.pop("Region"))
    df.insert(1, "Primary Center", df.pop("Primary Center"))
    df.insert(7, "Death Time", df.pop("Death Time"))
    df.insert(9, "Physical Discharge Remarks", df.pop("Physical Discharge Remarks"))
    df.insert(10, "Death Category", df.pop("Death Category"))

    # Drop columns
    df = df.drop(col_to_drop, axis=1)

    return df

# Generate Weekly data for international
def genWeeklyDeath(df, weekNum):
    df = df[df['Week'] == weekNum]

    df = df[[
        'Region',
        'Primary Center',
        'MR No.',
        'Patient Name W/O Title',
        'Sponsor',
        'Death Date',
        'Physical Discharge Remarks',
        'Death Reason',
        'Week'

    ]]
    # rename cols
    df = df.rename({
        'Primary Center' : 'Clinics',
        'Death Date' : 'Date of Death',
        'Physical Discharge Remarks' : 'Cause of Death',
        'Death Reason' : 'Cause of Death Grouped'

    }, axis=1)

    # Add Treatment column
    df['Treatment'] = 'Hemodialysis'
    # Relocate column
    df.insert(5, "Treatment", df.pop("Treatment"))

    # Convert Death date to string format
    df['Date of Death'] = df['Date of Death'].dt.strftime('%d/%m/%Y')

    # Sort df
    df = df.sort_values(by='Region')

    return df

# Count monthly deaths
def monthly_death_count(df):
    country_df = df.groupby('Month')['MR No.'].count().reset_index(name="Count")
    region_df = df.groupby(['Region', 'Month'])['MR No.'].count().reset_index(name="Count")
    center_df = df.groupby(['Region', 'Primary Center', 'Month'])['MR No.'].count().reset_index(name="Count")

    return country_df, region_df, center_df

# Count deaths reason
def monthly_death_reason(df):
    ## COUNTRY COUNT ##
    country_df = df.groupby(['Month', 'Death Category'])['MR No.'].count().reset_index(name="Count")

    # Calculate total deaths for each month
    total_counts = country_df.groupby('Month')['Count'].transform('sum')
    
    # Add a Percentage column
    country_df['Percentage'] = np.round((country_df['Count'] / total_counts) * 100,2)


    ## REGION COUNT ##
    region_df = df.groupby(['Region','Month', 'Death Category'])['MR No.'].count().reset_index(name="Count")

    # Calculate total deaths for each month
    total_counts = region_df.groupby(['Region', 'Month'])['Count'].transform('sum')

    # Add a Percentage column
    region_df['Percentage'] = np.round((region_df['Count'] / total_counts) * 100, 2)


    ## CENTER COUNT##
    center_df = df.groupby(['Region', 'Primary Center','Month', 'Death Category'])['MR No.'].count().reset_index(name="Count")

    # Calculate total deaths for each month
    total_counts = center_df.groupby(['Region', 'Primary Center', 'Month'])['Count'].transform('sum')

    # Add a Percentage column
    center_df['Percentage'] = np.round((center_df['Count'] / total_counts) * 100, 2)

    # Sort based on percentage
    center_df = center_df.sort_values(by=['Region', 'Primary Center', 'Percentage'])

    return country_df, region_df, center_df
