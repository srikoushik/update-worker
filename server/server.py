from flask import Flask, Response
import redis
app = Flask(__name__)

client = redis.Redis(host='redis', port=6379)

@app.route("/")
def main():
	return "Flask server is running!!"

@app.route("/getItemsbyColor/<color>")
def get_items_by_color(color):
	result = client.zrange(color, 0, 10)
	return Response(result, mimetype='application/json')

if __name__ == "__main__":
	app.run(debug=True, host='0.0.0.0')