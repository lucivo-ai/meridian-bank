# ============================================================
# Meridian Community Bank — Docker Image
# PostgreSQL 16 + MCP Server (read-only)
# ============================================================

# Stage 1: Fetch dump from Hetzner S3
FROM amazon/aws-cli:2.22.35 AS fetcher
ARG S3_ENDPOINT=https://nbg1.your-objectstorage.com
ARG S3_BUCKET=lucivo-bucket
ARG S3_KEY=meridian-bank-testdata/meridian_bank.dump

RUN --mount=type=secret,id=aws_access_key \
    --mount=type=secret,id=aws_secret_key \
    AWS_ACCESS_KEY_ID=$(cat /run/secrets/aws_access_key) \
    AWS_SECRET_ACCESS_KEY=$(cat /run/secrets/aws_secret_key) \
    aws s3 cp "s3://${S3_BUCKET}/${S3_KEY}" /tmp/meridian_bank.dump \
        --endpoint-url "${S3_ENDPOINT}"

# Stage 2: Final image
FROM postgres:16-alpine

# Install Python + MCP server
RUN apk add --no-cache python3 py3-pip && \
    pip install --break-system-packages postgres-mcp==0.3.0

# Postgres configuration
ENV POSTGRES_DB=meridian_bank \
    POSTGRES_USER=meridian \
    POSTGRES_PASSWORD=meridian_dev

# Copy schemas (used by initdb as fallback DDL, also documents the schema)
COPY schemas/source_systems/ /docker-entrypoint-initdb.d/01_source/
COPY schemas/data_warehouse/ /docker-entrypoint-initdb.d/02_warehouse/

# Copy dump from fetcher stage
COPY --from=fetcher /tmp/meridian_bank.dump /data/meridian_bank.dump

# Copy entrypoint
COPY entrypoint.sh /usr/local/bin/meridian-entrypoint.sh
RUN chmod +x /usr/local/bin/meridian-entrypoint.sh

EXPOSE 5432 8000

ENTRYPOINT ["meridian-entrypoint.sh"]
