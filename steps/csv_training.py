import csv
import os

file_path = r'C:\Users\Arun Raja\OneDrive\Desktop\Python_Training\SHARE_MARKET\Raw_Data'
file_name = 'snap_shot_ff.csv'

with open(file=os.path.join(file_path,file_name),mode='r') as file_read:
    csv_reader = csv.reader(file_read)
    