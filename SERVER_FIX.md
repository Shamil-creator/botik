# Решение проблемы git pull на сервере

## Проблема
На сервере файл `schedule_bot/schedule.db` отслеживается git и изменен локально, 
что вызывает конфликт при попытке сделать `git pull`.

## Решение на сервере

Выполните следующие команды **на сервере**:

### Шаг 1: Сохранить или удалить локальные изменения

**Вариант A: Удалить файл из индекса (рекомендуется)**
```bash
# Удалить schedule.db из индекса git (файл останется на диске)
git rm --cached schedule_bot/schedule.db

# Сделать коммит
git commit -m "Remove schedule.db from git tracking"

# Теперь можно сделать pull
git pull
```

**Вариант B: Сохранить изменения в stash**
```bash
# Сохранить текущие изменения
git stash

# Выполнить pull
git pull

# Удалить schedule.db из индекса (если он еще там)
git rm --cached schedule_bot/schedule.db 2>/dev/null || true

# Сделать коммит если нужно
git commit -m "Remove schedule.db from git tracking" || true

# Применить сохраненные изменения (если нужно)
git stash pop
```

**Вариант C: Принять удаление файла из удаленного репозитория**
```bash
# Удалить локальный файл (если он не важен)
rm -f schedule_bot/schedule.db

# Принять удаление из удаленного репозитория
git checkout HEAD -- schedule_bot/schedule.db 2>/dev/null || git rm schedule_bot/schedule.db

# Сделать коммит
git commit -m "Accept schedule.db removal from remote"

# Выполнить pull
git pull
```

### Шаг 2: Убедиться, что .gitignore обновлен

После `git pull` проверьте, что в `.gitignore` есть:
```
schedule_bot/schedule.db
*.db
*.db-journal
```

### Шаг 3: Проверить результат

```bash
# Проверить статус
git status

# Убедиться, что schedule.db больше не отслеживается
git ls-files | grep schedule.db
# (не должно быть вывода)
```

## Быстрое решение (один раз выполнить на сервере)

```bash
# 1. Удалить файл из индекса
git rm --cached schedule_bot/schedule.db

# 2. Сделать коммит
git commit -m "Remove schedule.db from git tracking"

# 3. Выполнить pull
git pull

# 4. Проверить что файл больше не отслеживается
git ls-files | grep schedule.db || echo "✓ Файл больше не отслеживается"
```

## После исправления

После выполнения этих команд:
- ✅ `schedule.db` больше не будет отслеживаться git
- ✅ Файл останется на диске и будет работать с ботом
- ✅ Проблема с `git pull` больше не возникнет
- ✅ Новые изменения в базе данных не будут конфликтовать с git

## Примечание

Команда `git rm --cached` удаляет файл только из индекса git, 
но файл остается на диске. Это означает, что база данных 
продолжит работать нормально, но git больше не будет 
отслеживать изменения в ней.


