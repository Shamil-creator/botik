#!/bin/bash
# Скрипт для выполнения на сервере для исправления проблемы с git pull

echo "=== Исправление проблемы с git pull на сервере ==="
echo ""

# Проверка наличия файла
if [ -f "schedule_bot/schedule.db" ]; then
    echo "✓ Файл schedule_bot/schedule.db существует на диске"
else
    echo "⚠ Файл schedule_bot/schedule.db не найден на диске"
fi

# Проверка, отслеживается ли файл
if git ls-files --error-unmatch schedule_bot/schedule.db >/dev/null 2>&1; then
    echo "⚠ Файл schedule_bot/schedule.db отслеживается git"
    echo ""
    echo "Удаление файла из индекса git..."
    git rm --cached schedule_bot/schedule.db
    echo "✓ Файл удален из индекса"
    echo ""
    echo "Создание коммита..."
    git commit -m "Remove schedule_bot/schedule.db from git tracking" || echo "⚠ Нет изменений для коммита"
else
    echo "✓ Файл schedule_bot/schedule.db уже не отслеживается git"
fi

echo ""
echo "Проверка .gitignore..."
if [ -f ".gitignore" ] && grep -q "schedule_bot/schedule.db" .gitignore; then
    echo "✓ .gitignore содержит правило для schedule.db"
else
    echo "⚠ .gitignore не содержит правила для schedule.db"
    echo "Добавьте в .gitignore:"
    echo "  schedule_bot/schedule.db"
    echo "  *.db"
fi

echo ""
echo "=== Готово! Теперь можно выполнить: git pull ==="
