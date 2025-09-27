FROM python:3.10

# Expose port 5000 for the Flask application
EXPOSE 5000

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the required Python packages
RUN pip install -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Command to run the Flask application
CMD ["python", "app.py"]