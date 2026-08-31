import os
import pandas as pd

# read files in the data folder and check if they are .csv files
from pathlib import Path

file_path = "data/order_item_options.csv"    #"data/order_items.csv"   # Single file check

df = pd.read_csv(file_path)

# Check counts
# print(f"Row count:  {df.shape[0]}")                                                         # 203519
# print(f"Unique Order_id count:  {df['ORDER_ID'].nunique()}")                                # 131328
# print(f"Unique Order_id, lineitem_id count: {df[['ORDER_ID','LINEITEM_ID']].nunique()}")    # ORDER_ID=131328, LINEITEM_ID=203518

# Check Nulls
# columns_to_check = ['USER_ID', 'PRINTED_CARD_NUMBER'] #, 'IS_LOYALTY']
# df = df[df[columns_to_check].notna()]
# df = df[df['USER_ID'].notna()]
# df = df[df['PRINTED_CARD_NUMBER'].notna()]      # True         46056
# df = df[df['PRINTED_CARD_NUMBER'].isna()]       # False        139655
# df = df[df['LINEITEM_ID'].isna()]       # False        139655
# print(f"LineItem Null count:  {df.shape[0]}")
# print(df[['USER_ID', 'PRINTED_CARD_NUMBER', 'IS_LOYALTY']])

# Group by checks
# df_counts = df.groupby(['ORDER_ID','LINEITEM_ID']).size().reset_index(name='record_count')
# df_counts = df.groupby(['LINEITEM_ID']).size().reset_index(name='record_count')
# df_counts = df_counts[df_counts['record_count'] != 1]
# print(df_counts.head())

print(df['OPTION_PRICE'].max())

   
#  5   OPTION_QUANTITY
# anomalies check
# df_anom = df[['ITEM_CATEGORY','ITEM_NAME','ITEM_QUANTITY','ITEM_PRICE']].sort_values(by=['ITEM_PRICE'], ascending=False)
# df_counts = df_counts[df_counts['record_count'] != 1]
# print(df_anom.head(20))

# print(df['ITEM_PRICE'].sort_values(ascending=False))
# print(df.sort_values(by='ITEM_PRICE', ascending=False))


# df = df[df['USER_ID'].isna()]
# df = df[df['PRINTED_CARD_NUMBER'].notna()]      # True         46056
# df = df[df['PRINTED_CARD_NUMBER'].isna()]       # False        17780
# df_counts = df.groupby('IS_LOYALTY').size().reset_index(name='record_count')
# print(df_counts)

# Combinations Check
# df = df[df['PRINTED_CARD_NUMBER'].notna()]        # True         46056
# df_user_per_card = df.groupby(['PRINTED_CARD_NUMBER','USER_ID']).size().reset_index(name='record_count')
# df_user_per_card2 = df_user_per_card
# df_joined = df_user_per_card.merge( df_user_per_card,
#                                    on='PRINTED_CARD_NUMBER',
#                                    how='inner')
# df_joined['same'] = df_joined['USER_ID_x'].where(df_joined['USER_ID_x'] != df_joined['USER_ID_y'])
# df_joined = df_joined[df_joined['same'].notna()]
# print(df_joined)
#
# df = df[df['USER_ID'].notna()]        # True         46056
# df_user_per_card = df.groupby(['USER_ID','PRINTED_CARD_NUMBER']).size().reset_index(name='record_count')
# df_user_per_card2 = df_user_per_card
# df_joined = df_user_per_card.merge( df_user_per_card,
#                                    on='USER_ID',
#                                    how='inner')
# df_joined['same'] = df_joined['PRINTED_CARD_NUMBER_x'].where(df_joined['PRINTED_CARD_NUMBER_x'] == df_joined['PRINTED_CARD_NUMBER_y'])
# df_joined = df_joined[df_joined['same'].notna()]
# print(df_joined)
#
# df = df[df['LINEITEM_ID'].notna()]        # True         46056
# df_user_per_card = df.groupby(['LINEITEM_ID','USER_ID']).size().reset_index(name='record_count')
# df_user_per_card2 = df_user_per_card
# df_joined = df_user_per_card.merge( df_user_per_card,
#                                    on='LINEITEM_ID',
#                                    how='inner')
# df_joined['same'] = df_joined['USER_ID_x'].where(df_joined['USER_ID_y'] != df_joined['USER_ID_x'])
# df_joined = df_joined[df_joined['same'].notna()]
# print(df_joined)





# blank_count = df['USER_ID'].isna().sum()
# print(f"Blank User_id Row count:  {blank_count}") 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# # Check file exists
# if not os.path.exists(file_path):
#     print("File is missing")
# else:
#     print("File exists")

#     # Check file size
#     size = os.path.getsize(file_path)
#     print(f"File size: {size:,} bytes")

#     if size == 0:
#         print("File is empty")

    # Try reading the CSV
    # try:
    #     df = pd.read_csv(file_path)                     # no encoding specified, may cause issues with special characters like single quotes
    #     df = pd.read_csv(file_path, encoding="cp1252")  # Specify encoding to handle special characters like single quotes
#         print("File can be read successfully")
#         print(f"Rows: {len(df):,}")
#         print(f"Columns: {len(df.columns)}")
#         print(df.columns.tolist())
#     except Exception as e:
#         print("File may be corrupted or invalid")
#         print(e)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# # Define your directory path
# directory_path = Path("data")

# # Filter only .csv files and store their names in a variable
# files_expected = [file.name for file in directory_path.iterdir() if file.is_file() and file.suffix == '.csv']

# # View the result
# # print(files_expected)

# for f in files_expected:
#     file_path = directory_path / f
#     print(f"Checking file: {file_path}")

#     # Check file exists
#     if not os.path.exists(file_path):
#         print(f"File is missing: {file_path}")
#     else:
#         # Check file size
#         size = os.path.getsize(file_path)
#         print(f"File size: {size:,} bytes")

#         if size == 0:
#             print("File is empty")

#         # Try reading the CSV
#         try:
#             df = pd.read_csv(file_path, encoding="cp1252")  # Specify encoding to handle special characters like single quotes
#             # print("File can be read successfully")
#             print(f"Rows: {len(df):,}")
#             print(f"Columns: {len(df.columns)}")
#             # print(df.head(2))  # Display the first 2 rows of the DataFrame
#             print(df.info())
#             # print(df.isnull().sum())
#             # print(df.columns.tolist())
#         except Exception as e:
#             print("File may be corrupted or invalid")
#             print(e)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# data\date_dim.csv
# Rows: 365
# Columns: 8
# RangeIndex: 365 entries, 0 to 364
# Data columns (total 8 columns):
#  #   Column        Non-Null Count  Dtype
# ---  ------        --------------  -----
#  0   date_key      365 non-null    str  
#  1   year          365 non-null    int64
#  2   month         365 non-null    int64
#  3   week          365 non-null    int64
#  4   day_of_week   365 non-null    str  
#  5   is_weekend    365 non-null    bool 
#  6   is_holiday    365 non-null    bool 
#  7   holiday_name  12 non-null     str  

# data\order_items.csv
# Rows: 203,519
# Columns: 13
# RangeIndex: 203519 entries, 0 to 203518
# Data columns (total 13 columns):
#  #   Column               Non-Null Count   Dtype  
# ---  ------               --------------   -----  
#  0   APP_NAME             203519 non-null  str    
#  1   RESTAURANT_ID        203519 non-null  str    
#  2   CREATION_TIME_UTC    203519 non-null  str    
#  3   ORDER_ID             203519 non-null  str    
#  4   USER_ID              185711 non-null  str    
#  5   PRINTED_CARD_NUMBER  46084 non-null   float64
#  6   IS_LOYALTY           203519 non-null  bool   
#  7   CURRENCY             203519 non-null  str    
#  8   LINEITEM_ID          203518 non-null  str    
#  9   ITEM_CATEGORY        203518 non-null  str    
#  10  ITEM_NAME            203518 non-null  str    
#  11  ITEM_PRICE           203519 non-null  float64
#  12  ITEM_QUANTITY        203519 non-null  int64  

# data\order_item_options.csv
# Rows: 193,017
# Columns: 6
# RangeIndex: 193017 entries, 0 to 193016
# Data columns (total 6 columns):
#  #   Column             Non-Null Count   Dtype  
# ---  ------             --------------   -----  
#  0   ORDER_ID           193017 non-null  str    
#  1   LINEITEM_ID        193017 non-null  str    
#  2   OPTION_GROUP_NAME  193017 non-null  str    
#  3   OPTION_NAME        193017 non-null  str    
#  4   OPTION_PRICE       193017 non-null  float64
#  5   OPTION_QUANTITY    193017 non-null  int64  
