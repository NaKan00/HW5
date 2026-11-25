FROM python:3.12.10-slim
WORKDIR /srv
COPY movie-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY movie-service/app ./app
COPY movie-service/tests ./tests
COPY schemas ./schemas
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
