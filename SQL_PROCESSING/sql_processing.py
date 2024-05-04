import mysql.connector
import json

from MONGO_PROCESSING.mongo_processing import insert_in_batches
from schemas.mongo_classes import Movies, Roles


def fetch_mysql_connect_data(query):
	conn = mysql.connector.connect(host='localhost', user='root', password='nhO2023FALL!?123', database='encora_challenge')
	cursor = conn.cursor()
	cursor.execute(query) 
	return cursor.fetchall()


def roles_json():
	"Reads mongodb schema"

	with open('./schemas/movies.json', 'r') as file:
		return json.load(file)


def get_total_rows():
	"Gets total rows of movies to be divided into 6 threads"

	data = fetch_mysql_connect_data('SELECT count(*) FROM movies')
	return data[0][0]


def get_all_movie_roles(movie_id, roles_schema):
	"Get all roles for each movie_id"


	roles_movie_conn = fetch_mysql_connect_data(
													f'SELECT roles.actor_id as actor_id, \
														roles.role as role FROM roles WHERE \
														roles.movie_id = {movie_id}'
												)
	
	all_roles = []
	each_role = roles_schema['actors'][0]
	for each in roles_movie_conn:

		each_role = each_role.copy()
		roles = Roles(each)

		each_role['actor_id'] = roles.actor_id
		each_role['role'] = roles.role


		all_roles.append(each_role)


	return all_roles


def fetch_all_roles(movies):
	"Combines movies with its roles and send it to be written by mongodb"

	roles_schema = roles_json()
	mongo_movies_roles = []
	count = 0
	check = True
	CAP = 500

	for each in movies:
		temp_dict = roles_schema.copy()
		movies = Movies(each)


		temp_dict['movie_id'] = movies.movie_id
		temp_dict['name'] = movies.name
		temp_dict['year'] = movies.year
		temp_dict['rankscore'] = movies.rankscore
		temp_dict['actors'] = get_all_movie_roles(movies.movie_id, roles_schema)

		mongo_movies_roles.append(temp_dict)
		
		count += 1
		if count == CAP:
			print('inserting...', mongo_movies_roles[-1])
			if not insert_in_batches(mongo_movies_roles, CAP):
				check = False
				break
			mongo_movies_roles = []
			count = 0
	
	return check


def fetch_data_from_sql(start, each_shard):
	"fetch data from sql and send it to fetch roles"

	movies = fetch_mysql_connect_data(f'SELECT * FROM movies LIMIT {each_shard} OFFSET {start}')
	print('movies count - ', len(movies))
	payload_output = fetch_all_roles(movies)
	
	if payload_output:
		print('Migration Successful!')
	else:
		print('Failure')

