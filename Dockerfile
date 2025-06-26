# Use official Python runtime
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Install Poetry and project dependencies
RUN pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false

# Copy only dependency files first for better caching
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-interaction --no-ansi

# Copy project
COPY . .

# Default command
CMD ["dagster", "dev", "-m", "dagster_pipeline", "--host", "0.0.0.0"]
