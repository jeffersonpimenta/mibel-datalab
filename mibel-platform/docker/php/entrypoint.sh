#!/bin/sh
set -e

echo "[entrypoint] A executar migrate.php..."
php /app/src/migrate.php || true   # falha não impede o arranque do php-fpm

echo "[entrypoint] A iniciar php-fpm..."
exec php-fpm
