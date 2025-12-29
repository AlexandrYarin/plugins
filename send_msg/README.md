# Flask-сервер для отправки писем через Yandex

## Описание

Flask-сервер принимает HTTP POST-запросы с определенного IP-адреса и использует класс `YandexSendMsg` для отправки электронных писем через SMTP-сервер Яндекса.

## Установка зависимостей

```bash
pip install -r requirements.txt
```

## Запуск сервера

```bash
# Установка разрешенного IP-адреса (по умолчанию 127.0.0.1)
export ALLOWED_IP=192.168.1.100

# Запуск сервера
python app.py
```

Или с использованием скрипта:

```bash
ALLOWED_IP=192.168.1.100 ./run_server.sh
```

## API Endpoints

### POST /send_email

Отправляет электронное письмо.

#### Headers:
- Content-Type: application/json

#### Request body:
```json
{
  "password": "пароль_приложения_яндекс",
  "template": "HTML-шаблон_письма_с_плейсхолдерами",
  "values": {
    "subject": "Тема письма",
    "sender": "отправитель@yandex.ru",
    "receiver": "получатель@example.com",
    "...": "другие_переменные_для_шаблона"
  },
  "mandatory_attach": true/false,
  "attachments": [
    {
      "type": "file|image",
      "name": "имя_файла_для_вложения",
      "content": "содержимое_файла_в_байтах",
      "url": "URL_изображения"
    }
  ]
}
```

#### Response:
- Успешно: `{"status": "success", "message": "Email sent successfully"}`
- Ошибка: `{"status": "error", "message": "описание_ошибки"}`

### GET /health

Проверяет состояние сервиса.

#### Response:
- `{"status": "healthy"}`

## Настройка

Сервер может быть настроен с помощью переменных окружения:

- `ALLOWED_IP` - разрешенный IP-адрес для доступа к API (по умолчанию 127.0.0.1)
- `LOG_LEVEL` - уровень логирования (по умолчанию INFO)
- `LOG_FILE` - файл для логов (по умолчанию flask_app.log)

## Пример использования

```bash
curl -X POST http://localhost:5000/send_email \
  -H "Content-Type: application/json" \
  -d @test_data.json
```