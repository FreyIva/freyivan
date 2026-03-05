е по выпуску # StroyControl — Строительный ERP

**Версия:** 1.5.10

Веб-приложение для управления строительством домов (каркасных, модульных, из газоблока, из пенополистиролбетона). Работает в локальной сети.

## Требования

- Python 3.8+

## Установка и запуск

1. **Создать виртуальное окружение:**
   ```bash
   python -m venv venv
   ```

2. **Активировать виртуальное окружение:**
   - Windows: `venv\Scripts\activate`
   - Mac/Linux: `source venv/bin/activate`

3. **Установить зависимости:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Заполнить тестовыми данными (15 проектов с этапами и чатом):**
   ```bash
   python seed_data.py
   ```

5. **Запустить приложение:**
   ```bash
   python app.py
   ```

6. **Открыть в браузере:**
   ```
   http://localhost:5002
   ```

   Для доступа из других устройств в локальной сети используйте IP сервера, например: `http://192.168.1.100:5000`

## Тестовые учётные записи

| Роль   | Логин     | Пароль     |
|--------|-----------|------------|
| Админ  | admin     | admin123   |
| Мастер | master    | master123  |
| Заказчик | client    | client123  |
| Менеджер ОП | manager_op | manager123 |

## История изменений

См. [CHANGELOG.md](CHANGELOG.md).

## Функциональность

- **Админ:** управление пользователями (мастера, клиенты), создание/редактирование/удаление проектов, настройка этапов, заявки на редактирование
- **Мастер:** просмотр своих объектов, добавление отчётов с фотографиями и комментариями
- **Заказчик:** просмотр своих объектов, прогресс-бар, галерея фото с возможностью открытия на весь экран

## Роли и права доступа

Подробное описание возможностей, подчинений и прав на редактирование по ролям — в файле **docs/ROLES_AND_PERMISSIONS.txt**. Обновляйте его при каждом изменении ролей или доступов.

## Структура проекта

```
stroycontrol/
├── app.py                  # Основное приложение Flask (роуты, БД, логика)
├── database.db             # SQLite — проекты, пользователи, этапы (создаётся при запуске)
├── media.db                # SQLite — медиафайлы (договоры, сметы, фото отчётов)
├── requirements.txt
├── seed_data.py            # Заполнение тестовыми данными
├── migrate_media.py        # Миграция медиа в защищённое хранилище
├── restore_uploads.py      # Восстановление загрузок
│
├── docs/
│   └── ROLES_AND_PERMISSIONS.txt   # Роли, подчинения, права доступа
│
├── static/
│   ├── css/style.css
│   └── js/
│       ├── admin.js        # Календарь, Gantt
│       ├── master.js       # Отчёты, заявки
│       └── client.js       # Галерея фото
│
├── storage/                # Защищённое хранилище медиа (projects/<id>/...)
│
└── templates/
    ├── base.html           # Базовый шаблон
    ├── login.html
    ├── notifications.html # Уведомления (согласование)
    ├── admin/              # Админ, менеджер ОП
    │   ├── dashboard.html, projects.html, project_edit.html
    │   ├── project_chat.html, stages.html
    │   ├── approvals.html  # Согласование документов
    │   ├── users.html, user_edit.html, roles.html
    │   ├── production_calendar.html, production_report.html
    │   ├── work_items.html, edit_requests.html
    │   └── daily_report_edit.html
    ├── director/           # Директора по производству и строительству
    │   ├── production_dashboard.html, production_calendar.html
    │   ├── production_approvals.html, production_analytics.html
    │   ├── construction_dashboard.html, construction_calendar.html
    │   ├── stage_confirmations.html, project_chat.html
    │   └── ...
    ├── master/             # Мастер
    │   ├── dashboard.html, project.html, project_chat.html
    │   └── work_approvals.html
    ├── foreman/            # Прораб
    │   ├── dashboard.html, project.html, project_chat.html
    ├── worker/             # Работник
    │   ├── dashboard.html, project.html
    ├── client/             # Заказчик
    │   ├── dashboard.html, project.html
    └── reports/            # Отчёты работников и прорабов
        ├── worker_reports.html, foreman_reports.html
```