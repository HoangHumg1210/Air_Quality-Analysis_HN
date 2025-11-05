import json

try:
    output_file = 'Models.json'

    with open('src/Models.ipynb', mode='r', encoding='utf-8') as f:
        my_file = json.loads(f.read())

    with open(output_file, mode='w', encoding='utf-8') as f:
        json.dump(my_file, f, indent=2)

except FileNotFoundError:
    print("Input file not found")
except json.JSONDecodeError:
    print("Invalid JSON format in input file")
except Exception as e:
    print(f"An error occurred: {str(e)}")
