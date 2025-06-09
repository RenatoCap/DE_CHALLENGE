FROM python:3.12-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y \
    unixodbc-dev \
    gcc \
    g++ \
    apt-transport-https \
    curl \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg
RUN curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && apt-get install -y \
    msodbcsql18 \
    unixodbc \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:create_app()"]