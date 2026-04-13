FROM php:8.3-fpm

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libzip-dev \
    libcurl4-openssl-dev \
    libonig-dev \
    sqlite3 \
    libsqlite3-dev \
    unzip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install PHP extensions
RUN docker-php-ext-install \
    curl \
    zip \
    pdo_sqlite \
    mbstring

# Install Composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

# Custom PHP configuration
RUN echo "upload_max_filesize = 512M" > /usr/local/etc/php/conf.d/custom.ini \
    && echo "post_max_size = 512M" >> /usr/local/etc/php/conf.d/custom.ini \
    && echo "max_execution_time = 0" >> /usr/local/etc/php/conf.d/custom.ini \
    && echo "memory_limit = 512M" >> /usr/local/etc/php/conf.d/custom.ini

WORKDIR /var/www/html

# Install phpoffice/phpspreadsheet
RUN composer require phpoffice/phpspreadsheet:^2.0 --no-interaction --prefer-dist
