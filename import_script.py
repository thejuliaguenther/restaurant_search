from algoliasearch import algoliasearch
import json
import codecs

client = algoliasearch.Client('QK3YR6EF4D', '7efafa747922320a9a4cc340172f87a6')
index = client.init_index('Restaurants')


def open_csv():
	try:
		restaurant_csv = open("./project-files/resources/dataset/restaurants_info.csv", "r")
	except IOError:
		print("CSV file not found")
		return
	else:
		return restaurant_csv

def open_json():
	try:
		restaurant_json = codecs.open("./project-files/resources/dataset/restaurants_list.json","r","utf-8")
	except IOError:
		print("JSON file not found")
		return
	else:
		restaurant_json_arr = json.loads(restaurant_json.read())
		
		restaurant_dict = {}

		# parse the data into a dictionary by ObjectID so all have O(1) lookups

		for x in range(len(restaurant_json_arr)):
			cards_set = set(restaurant_json_arr[x]['payment_options'])
			if 'Diners Club' in cards_set:
				cards_set.remove('Diners Club')
				cards_set.add('Discover')
			if 'Carte Blanche' in cards_set:
				cards_set.remove('Carte Blanche')
				cards_set.add('Discover')
			if 'JCB' in cards_set:
				cards_set.remove('JCB')
			card_list = list(cards_set)
			restaurant_json_arr[x]['payment_options'] = card_list
			restaurant_dict[restaurant_json_arr[x]["objectID"]] = restaurant_json_arr[x]
		return(restaurant_dict)

def combine_files():
	restaurant_csv = open_csv()
	restaurant_json = open_json()

	restaurant_info = []
	record_count = 0
	inside_index = 0

	next(restaurant_csv)
	for line in restaurant_csv:
		line = line.strip()
		line_items = line.split(';')
		object_id = line_items[0]
		food_type = line_items[1]
		stars_count = line_items[2]
		reviews_count = line_items[3]
		price_range = line_items[6]
		dining_style = line_items[7]

		details = restaurant_json[int(object_id)]
		details["food_type"] = food_type
		details["stars_count"] = stars_count
		details["reviews_count"] = reviews_count
		details["price_range"] = price_range
		details["dining_style"] = dining_style

		if record_count == 0:
			restaurant_info.append([details])
		elif record_count % 1000 == 0:
			restaurant_info.append([details])
			inside_index += 1
		else:
			restaurant_info[inside_index].append(details)

		record_count +=1
	return restaurant_info

def send_data():
	#send the chunked info to Algolia
	all_restaurant_info = combine_files()
	for chunk in all_restaurant_info:
		index.add_objects(chunk)

send_data()

