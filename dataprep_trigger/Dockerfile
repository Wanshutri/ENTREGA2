# Imagen base
FROM python:3.9-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar dependencias
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copiar código fuente
COPY . .

# Expone el puerto que Cloud Run espera
EXPOSE 8080

# Usar Gunicorn para levantar Flask
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]
