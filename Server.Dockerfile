FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy project files
COPY pyproject.toml .
COPY config.py database.py ./
COPY api/ ./api/

# Install dependencies
RUN uv pip install --system -e .

# Set environment variables for ngrok
ARG NGROK_ENABLED
ARG NGROK_AUTH_TOKEN

# Install curl and gnupg for ngrok installation
RUN if [ "$NGROK_ENABLED" = "true" ] && [ -n "$NGROK_AUTH_TOKEN" ]; then apt update && apt install -y curl gnupg -y; fi

# Install ngrok
RUN if [ "$NGROK_ENABLED" = "true" ] && [ -n "$NGROK_AUTH_TOKEN" ]; then \
      curl -sSL https://ngrok-agent.s3.amazonaws.com/ngrok.asc \
      | tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null \
      && echo "deb https://ngrok-agent.s3.amazonaws.com bookworm main" \
      | tee /etc/apt/sources.list.d/ngrok.list >/dev/null \
      && apt update \
      && apt install -y ngrok; \
    fi

# Configure ngrok with your authtoken
RUN if [ "$NGROK_ENABLED" = "true" ] && [ -n "$NGROK_AUTH_TOKEN" ]; then ngrok config add-authtoken $NGROK_AUTH_TOKEN; fi

CMD ["/bin/bash"]
