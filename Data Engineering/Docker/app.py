from flask import Flask
import boto3

app = Flask(__name__)

@app.route('/')
def home():
    return "Hello, Flask with Debugger!"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
    app.run(debug=True)
