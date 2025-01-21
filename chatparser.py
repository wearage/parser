from pyrogram import Client, enums
import csv
import re
import asyncio
from pyrogram.raw import functions
import time
import logging
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Список аккаунтов с указанием имен файлов сессии
accounts = [{'session_name': '+79863547135'}]

# Инициализация клиента с использованием существующего файла сессии
client = Client(accounts[0]['session_name'])

# Функция очистки текста
def clean_text(text):
    text = re.sub(r'\s+', ' ', text)  # Убираем лишние пробелы и переносы строк
    text = re.sub(r'[\\/:*?"<>|]', '', text)  # Убираем запрещенные символы для имени файла
    return text

# Функция для записи информации о чате в CSV файл
def write_chat_to_csv(chat_title):
    with open('чаты отработал.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([chat_title])

# Функция для проверки, был ли пользователь уже добавлен
def is_user_already_parsed(username, parsed_users):
    return username in parsed_users

# Функция для парсинга пользователей за последние 30 дней
async def parse_recent_users(chat_id, days=30):
    recent_users = set()
    cutoff_date = datetime.now() - timedelta(days=days)

    async for message in client.get_chat_history(chat_id):
        if message.date < cutoff_date:
            break

        if message.from_user:
            recent_users.add(message.from_user.id)

    return recent_users

# Основная функция для парсинга сообщений из выбранной группы
async def main():
    async with client:
        groups = []

        async for dialog in client.get_dialogs():
            logging.info(f"Chat: {dialog.chat.title if dialog.chat.title else 'No title'}, Type: {dialog.chat.type}")
            if dialog.chat.type in [enums.ChatType.SUPERGROUP, enums.ChatType.GROUP]:
                groups.append(dialog.chat)

        if not groups:
            logging.warning("Нет доступных групп для парсинга.")
            return

        print("Выберите группу для парсинга сообщений:")
        for i, group in enumerate(groups):
            print(f"{i} - {group.title}")

        try:
            g_index = int(input("Введите нужную цифру: "))
            target_group = groups[g_index]
            logging.info(f"Вы выбрали группу: {target_group.title}")
        except (IndexError, ValueError):
            logging.error("Неверный ввод или группа не найдена.")
            return

        # Записываем название выбранного чата в CSV файл
        write_chat_to_csv(target_group.title)

        group_name_cleaned = re.sub(r'[^\w\s-]', '', target_group.title).replace(' ', '_')
        filename = f"{group_name_cleaned}_users.csv"

        # Загружаем уже спарсенных пользователей, чтобы избежать дублирования
        parsed_users = set()
        try:
            with open(filename, "r", encoding="UTF-8") as f:
                reader = csv.reader(f, delimiter=",")
                for row in reader:
                    if row:
                        parsed_users.add(row[0])
        except FileNotFoundError:
            pass

        logging.info(f"Сохраняем данные в файл {filename}...")

        recent_users = await parse_recent_users(target_group.id, days=30)
        with open(filename, "a", encoding="UTF-8") as f:
            writer = csv.writer(f, delimiter=",", lineterminator="\n")

            for user_id in recent_users:
                try:
                    peer = await client.resolve_peer(user_id)
                    full_user = await client.invoke(functions.users.GetFullUser(id=peer))
                    username = full_user.user.username if full_user.user.username else ""
                    first_name = full_user.user.first_name if full_user.user.first_name else ""
                    bio = clean_text(full_user.full_user.about if full_user.full_user.about else "")

                    if username and username not in parsed_users:
                        logging.info(f"Username: {username}, First Name: {first_name}, Bio: {bio}")
                        writer.writerow([username, first_name, bio])
                        parsed_users.add(username)

                    # Увеличение задержки между запросами, чтобы избежать Flood Wait
                    await asyncio.sleep(1.5)  # Задержка 1.5 секунды
                except Exception as e:
                    if 'FLOOD_WAIT' in str(e):
                        wait_time = int(re.search(r'\d+', str(e)).group())
                        logging.warning(f"Flood wait detected. Waiting for {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        logging.error(f"Ошибка при обработке пользователя {user_id}: {e}")

        logging.info(f'Парсинг пользователей группы "{target_group.title}" успешно выполнен.')

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
