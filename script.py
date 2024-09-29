import csv
import random
filename = 'mathematical_operations1.csv'

operators = ['*', '/', '+', '-']

def compute_result(op, num1, num2):
    if op == '*':
        return num1 * num2
    elif op == '/':
        return num1 // num2
    elif op == '+':
        return num1 + num2
    elif op == '-':
        return num1 - num2

def generate_records(chunk_size, total_records):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Sr No", "Mathematical Operator", "Number 1", "Number 2", "Answer"])

        record_count = 0
        while record_count < total_records:
            records = []
            for i in range(chunk_size):
                num1 = random.randint(1, 1000)
                num2 = random.randint(1, 1000)
                operator = random.choice(operators)
                answer = compute_result(operator, num1, num2)
                records.append([record_count + 1, operator, num1, num2, answer])
                record_count += 1
                if record_count >= total_records:
                    break
            writer.writerows(records)

generate_records(chunk_size=5000000, total_records=5000000)
