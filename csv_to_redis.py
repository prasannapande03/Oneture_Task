import csv
import redis

r = redis.Redis(host='127.0.0.1', port=6379, db=0)

csv_file_path = 'mathematical_operations1.csv'

with open(csv_file_path, mode='r') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        sr_no = row['Sr No']  
        value = f"{row['Mathematical Operator']}, {row['Number 1']}, {row['Number 2']}, {row['Answer']}"
        
        
        r.set(sr_no, value)

print("Records have been written to Redis.")