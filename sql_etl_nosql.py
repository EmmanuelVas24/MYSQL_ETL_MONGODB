import csv,json, pymysql, datetime, pymongo, time
from pymysql.constants import CLIENT

conn = pymysql.connect(host='host', port=9999, user='user', passwd='pwd', db='db' ,) #setup our credentials to mysql, CHANGED FOR GITHUB

uri = 'uri' # set up connection credentials to mongoDB
client = pymongo.MongoClient(uri)
db = client['mydata_large'] #specify the mongoDB colleciton set ("database") name
obj = db["imdb"] #specify the collection ("table") name

cur3 = conn.cursor(pymysql.cursors.DictCursor)

sql ='SELECT `movie_id` FROM `movies`'
cur3.execute(sql)
for mid in cur3:
    print('movie_id: ', mid['movie_id'])
    cur = conn.cursor(pymysql.cursors.DictCursor)
    sql = 'SELECT *  FROM `movies` WHERE `movie_id` = %s'
    cur.execute(sql,mid['movie_id'])
    for m in cur:
        data = {}
        data['id'] = m['movie_id']
        data['imdb_id'] = m['imdb_id']
        data['title'] = m['movie_title']
        data['budget'] = m['budget']
        data['original_language']= m['original_language']
        data['release_date'] = str(m['release_date'])
        data['runtime'] = m['runtime']
        data['revenue'] = m['revenue']
        data['popularity_score'] = float(m['popularity_score'])
        data['vote_average'] = float(m['vote_average'])
        data['vote_count'] = m['vote_count']

        mongo_movie = obj.insert_one(data)

#---------------------------------------------------------------

cur3 = conn.cursor(pymysql.cursors.DictCursor)

sql ='SELECT `movie_id` FROM `top100_movies`'
cur3.execute(sql)
for mid in cur3:
    print('movie_id: ', mid['movie_id'])
    cur = conn.cursor(pymysql.cursors.DictCursor)
    sql = 'SELECT *  FROM `top100_movies` WHERE `movie_id` = %s'
    cur.execute(sql,mid['movie_id'])
    for m in cur:
        data = {}
        data['id'] = m['movie_id']
        data['imdb_id'] = m['imdb_id']
        data['title'] = m['movie_title']
        data['budget'] = m['budget']
        data['original_language']= m['original_language']
        data['release_date'] = str(m['release_date'])
        data['runtime'] = m['runtime']
        data['revenue'] = m['revenue']
        data['popularity_score'] = float(m['popularity_score'])
        data['vote_average'] = float(m['vote_average'])
        data['vote_count'] = m['vote_count']
        mongo_movie = obj.insert_one(data)

#-----------------------------------------------------------------

sql ='SELECT `movie_id` FROM `top100_movies`'
cur3.execute(sql)

for mid in cur3:
    print('movie_id: ', mid['movie_id'])
    sql = 'SELECT `keyword_id`, `keyword_name` FROM `keywords` WHERE `movie_id` = %s'
    cur2 = conn.cursor(pymysql.cursors.DictCursor)
    cur2.execute(sql,mid)
    data['keywords'] = []
    for no in cur2:#no = nested object
        nop = {}  #nested object pair
        nop['name'] = no['keyword_name']
        nop['id'] = no['keyword_id']
        data['keywords'].append(nop)
    mongo_movie = obj.insert_one(data)

#------------------------------------------------------------------

client = pymongo.MongoClient(uri)
db = client['mydata_large'] #specify the mongoDB colleciton set ("database") name
obj = db["imdb_v3"] #specify the collection ("table") name

cur3 = conn.cursor(pymysql.cursors.DictCursor)

sql ='SELECT `movie_id` FROM `movies`'
cur3.execute(sql)
for mid in cur3:
    print('movie_id: ', mid['movie_id'])
    cur = conn.cursor(pymysql.cursors.DictCursor)
    sql = 'SELECT *  FROM `movies` WHERE `movie_id` = %s'
    cur.execute(sql,mid['movie_id'])
    for m in cur:
        data = {}
        data['id'] = m['movie_id']
        data['imdb_id'] = m['imdb_id']
        data['title'] = m['movie_title']
        data['budget'] = m['budget']
        data['original_language']= m['original_language']
        data['release_date'] = str(m['release_date'])
        data['runtime'] = m['runtime']
        data['revenue'] = m['revenue']
        data['popularity_score'] = float(m['popularity_score'])
        data['vote_average'] = float(m['vote_average'])
        data['vote_count'] = m['vote_count']
        sql = 'SELECT `keyword_id`, `keyword_name` FROM `keywords` WHERE `movie_id` = %s'
        cur2 = conn.cursor(pymysql.cursors.DictCursor)
        cur2.execute(sql,m['movie_id'])
        data['keywords'] = []
        for no in cur2:#no = nested object
            nop = {}  #nested object pair
            nop['name'] = no['keyword_name']
            nop['id'] = no['keyword_id']
            data['keywords'].append(nop)

        sql = 'SELECT l.language_ISO, l.language_name_English FROM `languages_lib_of_congress` l, `movies_languages` ml WHERE ml.movie_id = %s AND ml.language_ISO = l.language_ISO'
        cur2 = conn.cursor(pymysql.cursors.DictCursor)
        cur2.execute(sql,m['movie_id'])
        data['languages'] = []
        for no in cur2:#no = nested object
            nop = {} #nested object pair
            nop['iso'] = no['language_ISO']
            nop['name'] = no['language_name_English']
            data['languages'].append(nop)

        sql = 'SELECT g.genre_id, g.genre_name FROM `movies_genres` mg, `genres` g WHERE mg.movie_id = %s AND mg.genre_id = g.genre_id'
        cur2 = conn.cursor(pymysql.cursors.DictCursor)
        cur2.execute(sql,m['movie_id'])
        data['genres'] = []
        for no in cur2:#no = nested object
            nop = {} #nested object pair
            nop['id'] = no['genre_id']
            nop['name'] = no['genre_name']
            data['genres'].append(nop)

        sql = 'SELECT pc.production_company_id, pc.production_company_name FROM `movies_production_companies` mpc, `production_companies` pc WHERE mpc.movie_id = %s AND mpc.production_company_id = pc.production_company_id'
        cur2 = conn.cursor(pymysql.cursors.DictCursor)
        cur2.execute(sql,m['movie_id'])
        data['production_companies'] = []
        for no in cur2:#no = nested object
            nop = {} #nested object pair
            nop['id'] = no['production_company_id']
            nop['name'] = no['production_company_name']
            data['production_companies'].append(nop)

        sql = 'SELECT mc.credit_id, mc.department, mc.job, fw.worker_id, fw.worker_name FROM `movie_credits` mc, `film_workers` fw WHERE mc.movie_id = %s AND mc.worker_id = fw.worker_id AND job<>"Acting"'
        cur2 = conn.cursor(pymysql.cursors.DictCursor)
        cur2.execute(sql,m['movie_id'])
        data['movie_credits'] = []
        for no in cur2:#no = nested object
            nop = {} #nested object pair
            nop['id']=no['credit_id']
            nop['department']=no['department']
            nop['job']=no['job']
            nop['worker_id'] = no['worker_id']
            nop['name'] = no['worker_name']
            data['movie_credits'].append(nop)

        sql = 'SELECT mc.credit_id, fw.worker_id, fw.worker_name, ac.character_name FROM `movie_credits` mc, `film_workers` fw, `acting_credits` ac WHERE mc.movie_id = %s AND mc.worker_id = fw.worker_id AND mc.credit_id = ac.credit_id'
        cur2 = conn.cursor(pymysql.cursors.DictCursor)
        cur2.execute(sql,m['movie_id'])
        for no in cur2:#no = nested object
            nop = {} #nested object pair
            nop['id']=no['credit_id']
            nop['actor_id'] = no['worker_id']
            nop['name'] = no['worker_name']
            nop['character_name']=no['character_name']
            data['movie_credits'].append(nop)
        mongo_movie = obj.insert_one(data)
        print(mongo_movie.inserted_id)
            
           # print(json.dumps(data))
