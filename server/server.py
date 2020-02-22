from flask import Flask, Response
import redis
import json
app = Flask(__name__)

client = redis.Redis(host='redis', port=6379)

@app.route("/")
def main():
	return "Flask server is running!!"

@app.route("/getItemsbyColor/<color>")
def get_items_by_color(color):
	result = client.zrevrange(color, 0, 9)
	response = transform_data_to_array(result)
	return Response(json.dumps(response), content_type='application/json')

@app.route("/getRecentItem/<date>")
def get_recent_item(date):
	result = client.zrevrange(date, 0, 0)
	return Response(result, content_type='application/json')

def transform_data_to_array(data):
	transformed_data = []
	for item in data:
		transformed_data.append(json.loads(item))
	return transformed_data

if __name__ == "__main__":
	app.run(debug=True, host='0.0.0.0')