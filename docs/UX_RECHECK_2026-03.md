# Повторная проверка UX — Десктоп и мобильные (март 2026)

## Десктоп

### ✅ Реализовано

| Элемент | Статус | Где |
|---------|--------|-----|
| **Skip-link** | ✅ | base.html, style.css — «Перейти к контенту», виден при Tab |
| **scope="col"** | ✅ Частично | stage_confirmations, worker/project, users, foreman_reports, master/project, login, admin/projects, admin/dashboard |
| **Индикатор загрузки** | ✅ | base.html — disabled + «Сохранение…» при submit (кроме data-no-loading) |
| **beforeunload** | ✅ | base.html — формы с data-unsaved-warn |
| **data-unsaved-warn** | ✅ | user_edit, project_edit, projects, stages, worker close-day, foreman substage |
| **Группировка кнопок** | ✅ | admin/stages — page-header-actions + sep перед «Удалить» |
| **Toast вместо alert** | ✅ | worker/project, foreman/project — showToast() |
| **Skeleton CSS** | ✅ | style.css — .skeleton, .skeleton-text, .skeleton-title, .skeleton-card |
| **empty-state** | ✅ | Улучшены стили |
| **focus-visible** | ✅ | nav-link, breadcrumbs, btn, form-control, modal-close, project-card, project-card-link, project-card-mini |
| **Command Palette** | ✅ | base.html — Ctrl+K / Cmd+K для быстрой навигации |

### 🟡 Таблицы без scope="col"

Добавить `scope="col"` для доступности в:

- **foreman/project.html** — таблицы «Работа/%/Комментарий» и «История закрытий»
- **foreman_reports.html** — заголовок «Прораб / Сумма» (строка 121)
- **admin/user_edit.html** — таблицы прав доступа
- **reports/worker_reports.html** — заголовки wr-list (data-col есть, scope нет)
- **director/construction_dashboard.html**
- **foreman/dashboard.html**, **client/dashboard.html**, **master/dashboard.html**
- **admin/approvals.html**, **admin/takeover_requests.html**, **admin/work_items.html**, **admin/roles.html**
- **admin/daily_report_edit.html**, **admin/production_report.html**
- **director/production_***, **manager_op/takeover.html**
- **reports/amocrm_*** — много таблиц

### 🟡 Не реализовано (низкий приоритет)

- Inline-валидация форм
- Пагинация / «Показать ещё» для длинных списков
- Сортировка по клику на заголовки (data-sortable есть в admin/projects, логика может быть)
- Select с поиском для client_id, master_id
- Sticky footer с кнопками в длинных формах
- Focus trap в модалках (фокус при открытии, Tab не выходит)
- aria-expanded, aria-haspopup для dropdown

---

## Мобильные

### ✅ Реализовано

| Элемент | Статус | Где |
|---------|--------|-----|
| **--color-dark** | ✅ | :root light/dark |
| **theme-toggle скрыт** | ✅ | display: none на ≤768px |
| **Safe area** | ✅ | header padding-top, footer padding-bottom (env(safe-area-inset-*)) |
| **Touch-зоны** | ✅ | stage-confirm, close-substage-btn — min-height: var(--touch-min) |
| **work-rows-wrap** | ✅ | Горизонтальный скролл, min-width 520px на ≤600px |
| **wr-list** | ✅ Частично | overflow-x: auto, -webkit-overflow-scrolling: touch — горизонтальный скролл |
| **Бургер-меню** | ✅ | nav-toggle, nav-open |
| **Viewport** | ✅ | viewport-fit=cover, theme-color, color-scheme |
| **font-size 16px input** | ✅ | Предотвращает зум iOS |

### 🟡 Рекомендации (не критично)

- **wr-list** — на ≤600px сейчас горизонтальный скролл. Аудит предлагал карточный вид — опционально для будущего.
- **login-hint-table** — min-width 520px, скролл есть.
- **capture="environment"** — для input[type=file] в модалке закрытия подэтапа (камера на мобильных).
- Pull-to-refresh, FAB, skeleton при загрузке — фаза 3.

---

## Чек-лист быстрой проверки

### Десктоп
- [ ] Tab — skip-link появляется, фокус идёт в main
- [ ] Tab по навигации — focus-visible на nav-link
- [ ] Submit формы — кнопка «Сохранение…», disabled
- [ ] Изменение формы с data-unsaved-warn → попытка уйти → предупреждение
- [ ] Ctrl+K — Command Palette открывается
- [ ] worker/project, foreman/project — toast вместо alert при ошибках

### Мобильные (эмуляция 375px)
- [ ] Бургер-меню открывается
- [ ] theme-toggle (1–5) скрыт
- [ ] Закрытие дня — таблица скроллится горизонтально
- [ ] Кнопки «Подтвердить этап», «Закрыть подэтап» — достаточный размер для нажатия
- [ ] Safe area — на iPhone X+ нет наложения на вырез

---

## Итог

**Десктоп:** Основные улучшения внедрены. Остались таблицы без scope (в основном отчёты, редко используемые страницы) и расширенные фичи (inline-валидация, пагинация).

**Мобильные:** Критичные пункты закрыты. wr-list с горизонтальным скроллом — приемлемое решение; карточный вид — опциональное улучшение.
