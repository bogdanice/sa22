from flask import Flask
from flask import Response
from datetime import datetime

app = Flask(__name__)


@app.route('/')
def index():
    return "Hello openshift"
 

@app.route("/health")
def getHealth():
    now = datetime.now().strftime("%H:%M:%S")
    upMsg = 'UP/{}'.format(now)
    resp = Response(upMsg)
    return resp

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='8080')