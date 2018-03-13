import psycopg2

query_1 = (
    "select articles.title, count(log.path) as views "
    "from articles inner join log on log.path "
    "like concat('%', articles.slug, '%')"
    "where log.status = '200 OK' group by "
    "articles.title, log.path order by views desc limit 3"
    )

query_2 = (
    "select authors.name, count(*) as views from articles "
    "inner join authors on articles.author = authors.id inner "
    "join log on log.path like concat('%', articles.slug, '%' )"
    "where log.status = '200 OK' group by authors.name "
    "order by views desc"
    )

query_3 = (
    "select day, perc from("
    "select day, round((sum(requests)/(select count(*) from log where "
    "substring(cast(log.time as text), 0, 11) = day) * 100), 2) as "
    "perc from (select substring(cast(log.time as text), 0, 11) as day, "
    "count(*) as requests from log where status = '404 NOT FOUND' group by day"
    ") as percentage group by day order by perc desc) as query where perc >= 1"
    )


def connect(database_name="news"):
    """connect to database"""
    db = psycopg2.connect("dbname={}".format(database_name))
    cursor = db.cursor()
    return db, cursor


def get_query_results(query):
    """Return query results for given query"""
    db, cursor = connect()
    cursor.execute(query)
    return cursor.fetchall()
    db.close()


def print_query_results(query_results):
    """prints results for first two queries"""
    for results in query_results:
        print(results[0], " - ", results[1], "views")


def print_error_results(query_results):
    """prints results for error query"""
    for results in query_results:
        print(results[0], results[1], "% errors")


if __name__ == '__main__':
    """store query results"""
popular_articles_results = get_query_results(query_1)
popular_authors_results = get_query_results(query_2)
load_error_days = get_query_results(query_3)
"""print query results"""
print("Top three articles by view count")
print_query_results(popular_articles_results)
print("Most popular Authors by view count")
print_query_results(popular_authors_results)
print("Days with more than 1% errors")
print_error_results(load_error_days)
