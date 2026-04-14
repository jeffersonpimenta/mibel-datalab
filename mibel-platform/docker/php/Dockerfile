FROM php:8.3-fpm

# Install system dependencies
# ADDED: libpng-dev, libjpeg-dev, libfreetype6-dev
RUN apt-get update && apt-get install -y \
    libzip-dev \
    libcurl4-openssl-dev \
    libonig-dev \
    sqlite3 \
    libsqlite3-dev \
    libpng-dev \
    libjpeg-dev \
    libfreetype6-dev \
    unzip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install PHP extensions
# ADDED: gd (with configuration for jpeg/freetype)
RUN docker-php-ext-configure gd --with-freetype --with-jpeg \
    && docker-php-ext-install \
    curl \
    zip \
    pdo_sqlite \
    mbstring \
    gd

# Install Composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

# Custom PHP configuration
RUN echo "upload_max_filesize = 512M" > /usr/local/etc/php/conf.d/custom.ini \
    && echo "post_max_size = 512M" >> /usr/local/etc/php/conf.d/custom.ini \
    && echo "max_execution_time = 0" >> /usr/local/etc/php/conf.d/custom.ini \
    && echo "memory_limit = 1G" >> /usr/local/etc/php/conf.d/custom.ini

WORKDIR /var/www/html

# Set Composer to ignore memory limits during install if it's still crashing
RUN COMPOSER_MEMORY_LIMIT=-1 composer require phpoffice/phpspreadsheet:^2.0 --no-interaction --prefer-dist

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]