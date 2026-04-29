import json

with open("api_original.json", "r", encoding="utf-8") as f:
    data = json.load(f)

with open("api_data.json", "w", encoding="utf-8") as f:
    for row in data:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

print("Archivo api_data.json generado correctamente")