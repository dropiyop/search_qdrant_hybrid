import json

# Открытие и чтение JSON файла
with open('filtered_data.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Удаление ключа 'username' из всех записей
for value in data:
    value.pop("username", None)

# Сохранение результата в новый файл
with open('example_data.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False, indent=4)

print("Преобразование завершено. Результат сохранен в файл 'example_data.json'")

#
# # Открытие и чтение JSON файла
# with open('example_data.json', 'r', encoding='utf-8') as file:
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


# import requests
# import time
# # Загружаем JSON-файл
# with open("data/transformed_data.json", "r", encoding="utf-8") as file:
#     data = json.load(file)
#
# # Отфильтрованные данные
# filtered_data = []
# total = len(data)
# # Проходим по каждой записи
# for i, entry in enumerate(data, start=1):
#     url = entry.get("source")
#     if url:
#         try:
#             response = requests.get(url, timeout=10)
#             if response.status_code != 401:
#                 filtered_data.append(entry)  # Добавляем только если статус не 401
#         except requests.RequestException:
#             pass  # Можно обработать ошибки соединения, если нужно
#
#             # Логируем процесс в процентах
#         progress = (i / total) * 100
#         print(f"Обработано {i} из {total} записей ({progress:.2f}%)")
#
# # Записываем отфильтрованные данные обратно в JSON
# with open("filtered_data.json", "w", encoding="utf-8") as file:
#     json.dump(filtered_data, file, ensure_ascii=False, indent=4)
#
# print("Фильтрация завершена. Сохранено записей:", len(filtered_data))
