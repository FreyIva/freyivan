# Аудит логики проекта StroyControl (март 2026)

## Шаг 1: Обзор архитектуры и ролей

### Цепочки подчинения
| Цепочка | Роли | Связь в БД |
|---------|------|------------|
| **Производство** | director_production → master → worker | reports_to_production_id, reports_to_id |
| **Строительство** | director_construction → foreman | reports_to_construction_id, foreman_project_access |

### Типы проектов
- `module` — модульные дома (производство)
- `frame`, `gasblock`, `penopolistirol` — стройка на участке

### Ключевые потоки (текущая реализация)
1. **Этапы прораба** → foreman закрывает подэтапы → stage_completions → director_construction подтверждает
2. **Этапы мастера** → master добавляет отчёты → stage_confirmed_at → director_production или master подтверждает
3. **Работы работника** → worker закрывает день → worker_daily_report_items (pending) → master (главный) или director_production подтверждает

### Статус шага 1
✅ Архитектура соответствует документации ROLES_AND_PERMISSIONS.txt

---

## Шаг 2: Потоки согласования

### Таблицы со статусом pending
| Таблица | Поле | Кто создаёт | Кто подтверждает |
|---------|------|-------------|------------------|
| stage_completions | status='pending' | foreman (submit_stage) | director_construction |
| worker_daily_report_items | approved_status='pending' | worker, foreman (модуль) | master, director_production |
| edit_requests | status='pending' | master, foreman | admin, director_production, director_construction |
| project_takeover_requests | status='pending' | manager_op | admin |

### Проверка маршрутов
- `stage_completion_confirm` — admin, director_construction ✓
- `stage_confirm` — master, director_production (модуль); director_construction (стройка) ✓
- `master_work_approvals` — master (reports_to_id) ✓
- `director_production_work_approvals` — director_production (reports_to_production_id) ✓

### Потенциальная несогласованность
- **worker_daily_report_items**: foreman может создавать pending при close_day для модульных проектов (если foreman назначен на module). Проверить: foreman_project_access обычно только для frame/gasblock/penopolistirol. Закрытие дня для foreman скрыто при construction — ок.

### Статус шага 2
✅ Потоки согласования согласованы с ролями

---

## Шаг 3: БД и миграции

### Инициализация
- `init_db()` вызывается в `before_request` при первом запросе (флаг `_db_initialized`)
- Миграции выполняются последовательно внутри `init_db`

### Подключение
- `get_db()` — новое соединение на каждый вызов, `sqlite3.Row` для результатов
- Нет пула соединений (для SQLite приемлемо)
- **Риск**: не все маршруты вызывают `conn.close()` при раннем return — возможна утечка соединений

### Ключевые таблицы
- `stage_completions` — этапы прораба на проверке
- `worker_daily_report_items` — approved_status, approved_by_id
- `substage_completions` — закрытие подэтапов с checklist_data
- `project_substages` — связь с construction_substage_templates

### Рекомендация
- Добавить `@app.after_request` или контекстный менеджер для гарантированного закрытия conn при ошибках

### Статус шага 3
✅ Миграции последовательные, структура согласована

---

## Шаг 4: Рекомендации по оптимизации и синхронизации

### 4.1 Управление соединениями
- **Проблема**: `conn.close()` не вызывается при раннем return в некоторых маршрутах
- **Решение**: обернуть `get_db()` в контекстный менеджер или использовать `@app.teardown_request` для закрытия соединений по request
- **Приоритет**: средний

### 4.2 Кэширование счётчиков
- **Проблема**: `inject_notifications` вызывает 4–5 SQL-запросов на каждый рендер страницы
- **Решение**: кэшировать `pending_*_count` на 30–60 сек (Redis или in-memory с TTL)
- **Приоритет**: низкий (если страницы грузятся быстро)

### 4.3 Индексы БД
- **Рекомендуемые индексы**:
  - `stage_completions(status)` — для выборки pending
  - `worker_daily_report_items(daily_report_id, approved_status)` — для отчётов
  - `edit_requests(status, master_id)` — для заявок по роли
- **Приоритет**: средний при росте данных

### 4.4 Синхронизация (race conditions)
- **Сценарий**: два директора одновременно подтверждают один stage_completion
- **Текущее состояние**: SQLite блокирует при записи; второй запрос дождётся
- **Рекомендация**: при переходе на PostgreSQL — использовать `SELECT ... FOR UPDATE` или optimistic locking
- **Приоритет**: низкий для текущей нагрузки

### 4.5 Дублирование логики
- **Проблема**: `get_pending_work_approvals_count_for_master` и `get_pending_work_approvals_count_for_director_production` — похожие запросы
- **Решение**: объединить в один `get_pending_work_approvals_count(role, user_id)` с ветвлением
- **Приоритет**: низкий (рефакторинг)

### 4.6 Документация
- **Рекомендация**: обновлять `ROLES_AND_PERMISSIONS.txt` при изменении потоков согласования
- **Ссылка**: `AUDIT_LOGIC_2026-03.md` — фиксация состояния на март 2026

### Статус шага 4
✅ Рекомендации зафиксированы

---

## Итог аудита

| Шаг | Статус |
|-----|--------|
| 1. Архитектура и роли | ✅ |
| 2. Потоки согласования | ✅ |
| 3. БД и миграции | ✅ |
| 4. Рекомендации | ✅ |

**Критичных проблем не выявлено.** Логика согласована с ролями и документацией.
