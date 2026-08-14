FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/plugins \
    BABEL_STATE_DIR=/state

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN addgroup --system --gid 10001 babel \
    && adduser --system --uid 10001 --ingroup babel --home /app babel \
    && mkdir -p /state \
    && chown -R babel:babel /app /state

COPY --chown=babel:babel . .

USER babel

EXPOSE 8080

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
