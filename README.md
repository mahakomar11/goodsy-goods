# Goodsy goods
![](https://upload.wikimedia.org/wikipedia/commons/thumb/4/40/Supermarket_full_of_goods.jpg/800px-Supermarket_full_of_goods.jpg)
REST API сервис, который позволяет загружать товары и категории, 
обновлять их и смотреть статистику за указанный интервал времени.

Swagger: https://meals-2046.usr.yandex-academy.ru/docs

### Как запустить

1. Установить docker и docker-compose.
2. Склонировать репозиторий:
```
git clone git@github.com:mahakomar11/goodsy-goods.git
cd goodsy-goods
```
3. Добавить файл .env c переменными окружения (см. .env-example)
4. Собрать и поднять docker-compose:
```
docker-compose up -d --build
```
5. Готово! Сервис развёрнут на 0.0.0.0:80.
6. Чтобы остановить:
```
docker-compose stop
```
7. Чтобы поднять заново (без изменения кода):
```
docker-compose up -d
```

При перезапуске машины docker-compose поднимет всё автоматически.