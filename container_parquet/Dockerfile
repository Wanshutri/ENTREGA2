FROM python:3.9-slim
WORKDIR /app
COPY main.py requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8080
CMD ["gunicorn", "-b", ":8080", "main:app"]