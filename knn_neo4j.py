from neo4j.v1 import GraphDatabase, basic_auth
from sklearn.neighbors import KDTree

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo"))


def nearest_neighbour(label):
    with driver.session() as session:
        result = session.run("""\
        MATCH (t:`%s`)
        RETURN t.id AS token, t.embedding AS embedding
        """ % label)

        points = {row["token"]: row["embedding"] for row in result}

    items = list(points.items())

    X = [item[1] for item in items]
    kdt = KDTree(X, leaf_size=10000, metric='euclidean')
    distances, indices = kdt.query(X, k=2, return_distance=True)

    for index, item in enumerate(items):
        nearest_neighbour_index = indices[index][1]
        distance = distances[index][1]
        print(item[0], items[nearest_neighbour_index][0], distance)


nearest_neighbour("Token")
