# Start with slim image for Python 3.14
FROM python:3.14-slim

# Copy uv binary from the official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory
WORKDIR /app

# Add virtual environment to PATH to use installed packages or use default system Python
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files
COPY pyproject.toml .python-version uv.lock ./

# Create virtual environment and install dependencies from lock file
RUN uv sync --locked
