FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN groupadd --system app && useradd --system --gid app --create-home app

COPY --chown=app:app src ./src
COPY --chown=app:app config ./config

USER app

ENTRYPOINT ["python", "-m", "fraud_streaming"]
CMD ["--config", "config/rules.json", "--events", "100", "--seed", "42", "--output-dir", "/tmp/artifacts"]

