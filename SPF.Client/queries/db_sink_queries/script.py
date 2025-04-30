import csv

# Open your CSV file
with open('sparql_2024-03-19_12-59-12Z.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Skip the header row if there is one
    for i, row in enumerate(reader, start=1):
        # Extract the SPARQL query (assuming it's in the first column)
        sparql_query = row[0]
        # Create a new file for each query
        with open(f'{i}.sparql', 'w') as file:
            file.write(sparql_query)

print("SPARQL files created successfully.")

