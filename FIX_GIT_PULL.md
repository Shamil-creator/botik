# Инструкция по исправлению ошибки git pull на сервере

## Проблема
При выполнении `git pull` возникает ошибка:
```
error: Your local changes to the following files would be overwritten by merge:
        schedule_bot/schedule.db
```

## Решение

Выполните следующие команды на сервере:

### Вариант 1: Удалить файл из индекса git (рекомендуется)

```bash
# 1. Удалить schedule.db из индекса git (файл останется на диске)
git rm --cached schedule_bot/schedule.db

# 2. Убедиться, что .gitignore содержит правило для *.db
# Если .gitignore еще не обновлен, обновите его

# 3. Сделать коммит удаления файла из индекса
git commit -m "Remove schedule.db from git tracking"

# 4. Теперь можно сделать pull
git pull
```

### Вариант 2: Сохранить локальные изменения и обновить

```bash
# 1. Сохранить текущие изменения в stash
git stash

# 2. Выполнить pull
git pull

# 3. Удалить schedule.db из индекса (если он еще там)
git rm --cached schedule_bot/schedule.db

# 4. Обновить .gitignore если нужно
# Убедитесь, что там есть:
#   schedule_bot/schedule.db
#   *.db

# 5. Сделать коммит
git commit -m "Remove schedule.db from git tracking"

# 6. Применить сохраненные изменения (если нужно)
git stash pop
```

### Вариант 3: Перезаписать локальные изменения (если база данных не важна)

```bash
# 1. Удалить локальный файл schedule.db
rm schedule_bot/schedule.db

# 2. Удалить из индекса git
git rm --cached schedule_bot/schedule.db

# 3. Сделать коммит
git commit -m "Remove schedule.db from git tracking"

# 4. Выполнить pull
git pull
```

## После исправления

Убедитесь, что в `.gitignore` есть следующие правила:
```
schedule_bot/schedule.db
*.db
*.db-journal
```

После этого `schedule.db` не будет отслеживаться git, и проблема больше не возникнет.


