from pyrogram import Client, enums
import csv
import re
import asyncio
from pyrogram.raw import functions
import time
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Список аккаунтов с указанием имен файлов сессии
accounts = [{'session_name': '+79863547135'}]

# Инициализация клиента с использованием существующего файла сессии
client = Client(accounts[0]['session_name'])

# Функция очистки текста
def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip() if text else ""

# Функция для проверки, уже ли пользователь был спарсен
def is_user_already_parsed(username, parsed_users):
    return username in parsed_users

async def main():
    async with client:
        # Укажите username группы
        target_group_username = input("Введите username группы (без @): ").strip()

        try:
            # Поиск группы по username
            target_group = await client.get_chat(target_group_username)
            logging.info(f"Группа найдена: {target_group.title}, ID: {target_group.id}")
        except Exception as e:
            logging.error(f"Ошибка при поиске группы: {e}")
            return

        # Проверяем наличие привязанного чата обсуждений
        if target_group.linked_chat:
            discussion_chat = await client.get_chat(target_group.linked_chat.id)
            logging.info(f"Обсуждение найдено: {discussion_chat.title}, ID: {discussion_chat.id}")
        else:
            logging.warning("Чат обсуждений не привязан к группе.")
            return

        group_name_cleaned = re.sub(r'[^\w\s-]', '', target_group.title).replace(' ', '_')
        filename = f"{group_name_cleaned}_discussion_users.csv"

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

        with open(filename, "a", encoding="UTF-8") as f:
            writer = csv.writer(f, delimiter=",", lineterminator="\n")

            async for message in client.get_chat_history(discussion_chat.id):
                if message.from_user is None:
                    continue

                username = message.from_user.username if message.from_user.username else ""
                first_name = message.from_user.first_name if message.from_user.first_name else ""

                if not username or is_user_already_parsed(username, parsed_users):
                    continue

                logging.info(f"Processing user {username} with first name {first_name}")

                try:
                    peer = await client.resolve_peer(message.from_user.id)
                    full_user = await client.invoke(functions.users.GetFullUser(id=peer))
                    bio = clean_text(full_user.full_user.about if full_user.full_user.about else "")

                    if bio:  # Если био есть, сохраняем пользователя в файл
                        logging.info(f"Username: {username}, First Name: {first_name}, Bio: {bio}")
                        writer.writerow([username, first_name, bio])
                        parsed_users.add(username)  # Добавляем пользователя в список уже спарсенных

                    # Увеличение задержки между запросами, чтобы избежать Flood Wait
                    await asyncio.sleep(1.5)  # Задержка 1.5 секунды
                except Exception as e:
                    if 'FLOOD_WAIT' in str(e):
                        wait_time = int(re.search(r'\d+', str(e)).group())
                        logging.warning(f"Flood wait detected. Waiting for {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        logging.error(f"Ошибка при обработке пользователя {message.from_user.id}: {e}")

        logging.info(f'Парсинг пользователей из обсуждений группы "{target_group.title}" успешно выполнен.')

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
