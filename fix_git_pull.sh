#!/bin/bash
# Скрипт для исправления проблемы с git pull на сервере

echo "=== Исправление проблемы с git pull ==="
echo ""
echo "Текущий статус git:"
git status --short

echo ""
echo "Удаление schedule.db из индекса git..."
git rm --cached schedule_bot/schedule.db 2>/dev/null || echo "Файл уже удален из индекса"

echo ""
echo "Проверка .gitignore..."
if grep -q "schedule_bot/schedule.db" .gitignore && grep -q "^\*\.db$" .gitignore; then
    echo "✓ .gitignore содержит правила для schedule.db"
else
    echo "⚠ .gitignore не содержит правил для schedule.db"
    echo "Добавьте следующие строки в .gitignore:"
    echo "  schedule_bot/schedule.db"
    echo "  *.db"
fi

echo ""
echo "Проверка изменений..."
if git diff --cached --quiet; then
    echo "Нет изменений для коммита"
else
    echo "Есть изменения для коммита. Выполните:"
    echo "  git commit -m 'Remove schedule.db from git tracking'"
fi

echo ""
echo "Теперь можно выполнить:"
echo "  git pull"
