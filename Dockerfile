FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .
COPY scaffold_generator.py .
COPY config.py .
COPY bdh_kernel.py .
COPY distillation_worker.py .
COPY llm_distillation_worker.py .
COPY distillation_runner.py .
ENV PORT=8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "60", "server:app"]
