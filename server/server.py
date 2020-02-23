from flask import Flask, Response
import redis
import json
app = Flask(__name__)

client = redis.Redis(host='redis', port=6379)

@app.route("/")
def main():
	return "Flask server is running!!"

@app.route("/items/color/<color>")
def get_items_by_color(color):
	result = client.zrevrange(color, 0, 9)
	response = transform_data_to_array(result)
	return Response(json.dumps(response), content_type='application/json')

@app.route("/items/recent/<date>")
def get_recent_item(date):
	result = client.zrevrange(date, 0, 0)
	return Response(result, content_type='application/json')

@app.route("/items/count/<date>")
def get_brands_count(date):
	result = client.zrevrange(date+"_count", 0, -1, 'withscores')
	response = transform_list_to_dict(result)
	return Response(json.dumps(response), content_type='application/json')

def transform_list_to_dict(data):
	transformed_data = []
	for item in data:
		key, value = item
		transformed_data.append(dict({key.decode("utf-8"): int(value)}))
	return transformed_data

def transform_data_to_array(data):
	transformed_data = []
	for item in data:
		transformed_data.append(json.loads(item))
	return transformed_data

if __name__ == "__main__":
	app.run(debug=True, host='0.0.0.0')