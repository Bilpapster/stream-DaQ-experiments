# Use Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements/requirements_visualizations.txt ./requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer source code
COPY output_consumer/ .

# Command to run the consumer script
CMD ["python", "visualizations.py"]