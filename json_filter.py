import json

# Открытие и чтение JSON файла
with open('example.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Создание нового словаря с измененной структурой
new_data = {}

# Обработка каждой записи
for key, value in data.items():
    new_record = {
        "type_source": "сайт",
        "source": value.get("url"),
        "tokens": None,
        "created_at": None,
        "username": None,
        "category_name": value.get("category_name"),
        "thread_name": value.get("thread_name"),
        "question": value.get("structured_content", {}).get("question"),
        "answer": value.get("structured_content", {}).get("answer")
        }

    # Добавление записи в новый словарь с тем же ключом
    new_data[key] = new_record

# Сохранение результата в новый файл
with open('example_data.json', 'w', encoding='utf-8') as file:
    json.dump(new_data, file, ensure_ascii=False, indent=4)

print("Преобразование завершено. Результат сохранен в файл 'example_data.json'")


# Открытие и чтение JSON файла
# with open('data/example_data.json', 'r', encoding='utf-8') as file:
#     content = file.read()
#     # Исправление последней запятой, если она есть
#     if content.rstrip().endswith(','):
#         content = content.rstrip()[:-1] + '}'
#     data = json.loads(content)
#
# # Создание нового списка с объектами без идентификаторов
# new_data = []
#
# # Добавление каждого объекта в список
# for key, value in data.items():
#     new_data.append(value)
#
# # Сохранение результата в новый файл
# with open('data/example_data.json', 'w', encoding='utf-8') as file:
#     json.dump(new_data, file, ensure_ascii=False, indent=4)
#
# print("Преобразование завершено. Результат сохранен в файл 'example_data.json'")