class Movies:
	def __init__(self, data):
		self.movie_id = data[0]
		self.name = data[1]
		self.year = data[2]
		self.rankscore = data[3]

class Roles:
	def __init__(self, data):
		self.actor_id = data[0]
		self.role = data[1]