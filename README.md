# project2

`project2` подготовлен для запуска на VPS или другом хостинге через `docker compose`.

Что внутри:

- `nginx` как внешний входной слой
- `start_service` как корневой сервис для `/`
- `users_service` с локальной SQLite
- `products_service` в двух экземплярах
- `RabbitMQ` для общения `products_service -> users_service`
- общая внешняя `PostgreSQL` база для `products_service`

## Что открыть наружу

На сервере нужен только один внешний порт:

- `80/tcp`

Порт `15672` у `RabbitMQ` проброшен только на `127.0.0.1`, чтобы админка не торчала наружу.

## Подготовка

На сервере должны быть установлены:

- `Docker`
- `Docker Compose`

Скопируй `.env.example` в `.env`:

```bash
cp .env.example .env
```

Заполни в `.env`:

- `RABBITMQ_DEFAULT_USER`
- `RABBITMQ_DEFAULT_PASS`
- `RABBITMQ_URL`
- `PRODUCTS_DATABASE_URL`

## Запуск

Из папки проекта:

```bash
docker compose up -d --build
```

Проверка:

- `http://YOUR_SERVER_IP/`
- `http://YOUR_SERVER_IP/health`
- `http://YOUR_SERVER_IP/products`
- `http://YOUR_SERVER_IP/users`

## Полезные команды

Логи:

```bash
docker compose logs -f
```

Пересборка:

```bash
docker compose up -d --build
```

Остановка:

```bash
docker compose down
```

## Как это роутится

- `/` -> `start_service`
- `/health` -> `start_service`
- `/users` -> `users_service`
- `/products` -> `products_service_1` или `products_service_2`

## Важная заметка

`users_service` сейчас хранит данные в локальном Docker volume через SQLite. Для учебного или небольшого VPS-сценария это нормально. Если хочешь полноценный production-вариант, следующим шагом лучше перевести и `users_service` на `PostgreSQL`.
