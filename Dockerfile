FROM postgres:15.6

# COPY ./docker/utc_set.sql /docker-entrypoint-initdb.d/

ENV POSTGRES_PASSWORD=postgres
ENV TZ='Europe/Moscow'
