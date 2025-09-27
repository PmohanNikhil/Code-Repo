FROM python:3.14.0b1-slim

WORKDIR /

COPY requirements.txt  requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app/

# Expose the port the app runs on
EXPOSE 5000

# Run the application   
CMD ["python", "app.py"]