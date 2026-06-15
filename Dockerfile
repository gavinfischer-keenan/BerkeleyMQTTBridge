FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e .
ENV PYTHONUNBUFFERED=1
CMD ["berkeley-mqtt-bridge"]
