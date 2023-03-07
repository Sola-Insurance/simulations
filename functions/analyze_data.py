import csv

with open('../files/output/Net Loss Ratio.csv') as file_obj:
    # Create reader object by passing the file
    # object to reader method
    reader_obj = csv.reader(file_obj)

    loss_list = []

    exceedance_table = {}

    # Iterate over each row in the csv
    # file using reader object
    for row in reader_obj:
        if row[1] != "Total":
            loss_list.append(row[1])

    loss_list.sort()

    total = len(loss_list)

    increment = int(total / 20)

    start = increment - 1

    while start < total:
        exceedance_table[f'{100 - round((start/total) * 100)}%'] = loss_list[start]
        start += increment

    print(exceedance_table)

