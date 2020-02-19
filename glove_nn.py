import sys

from neo4j import GraphDatabase, basic_auth
from sklearn.neighbors import KDTree

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo"), encrypted=False)


def nearest_neighbour(label):
    with driver.session() as session:
        result = session.run("""\
        MATCH (t:`%s`)
        RETURN id(t) AS token, t.embedding AS embedding
        """ % label)

        points = {row["token"]: row["embedding"] for row in result}
        items = list(points.items())

        X = [item[1] for item in items]
        print("[NN] building KDTree")
        kdt = KDTree(X, leaf_size=10000, metric='euclidean')
        print("[NN] finding nearest neighbours")
        distances, indices = kdt.query(X, k=2, return_distance=True)

        print("[NN] building params")
        params = []
        for index, item in enumerate(items):
            nearest_neighbour_index = indices[index][1]
            distance = distances[index][1]

            t1 = item[0]
            t2 = items[nearest_neighbour_index][0]
            params.append({"t1": t1, "t2": t2, "distance": distance})

        print("[NN] executing Cypher")
        session.run("""\
        UNWIND $params AS param
        MATCH (token) WHERE id(token) = param.t1
        MATCH (closest) WHERE id(closest) = param.t2
        MERGE (token)-[nearest:NEAREST_TO]->(closest)
        ON CREATE SET nearest.weight = param.distance
        """, {"params": params})


def union_find(label, round=None):
    print("Round:", round, "label: ", label)
    with driver.session() as session:
        result = session.run("""\
            CALL algo.unionFind.stream(
              "MATCH (n:`%s`) RETURN id(n) AS id", 
              "MATCH (a:`%s`)-[:NEAREST_TO]->(b:`%s`) RETURN id(a) AS source, id(b) AS target", 
              {graph: 'cypher'}
            )
            YIELD nodeId, setId
            MATCH (token) WHERE id(token) = nodeId
            MERGE (cluster:Cluster {id: setId, round: $round })
            MERGE (cluster)-[:CONTAINS]->(token)
            """ % (label, label, label), {"label": label, "round": round})

    print(result.summary().counters)


def check_clusters(round):
    with driver.session() as session:
        query = "MATCH (n:Cluster) WHERE n.round = $round RETURN count(*) AS clusters"
        return session.run(query, {"round": round}).peek()["clusters"]


def macro_vertex(macro_vertex_label, round=None):
    with driver.session() as session:
        result = session.run("""\
            MATCH (cluster:Cluster)
            WHERE cluster.round = $round
            RETURN cluster
            """, {"round": round})

        for row in result:
            cluster_id = row["cluster"]["id"]
            tx = session.begin_transaction()
            tx.run("""\
                MATCH (cluster:Cluster {id: $clusterId, round: $round })-[:CONTAINS]->(token)
                WITH cluster, collect(token) AS tokens
                UNWIND tokens AS t1 UNWIND tokens AS t2 WITH t1, t2, cluster WHERE t1 <> t2
                WITH t1, cluster, reduce(acc = 0.0, t2 in collect(t2) | acc + apoc.algo.euclideanDistance(t1.embedding, t2.embedding)) AS distance
                WITH t1, cluster, distance ORDER BY distance LIMIT 1
                SET cluster.centre = t1.id
                WITH t1
                CALL apoc.create.addLabels(t1, [$newLabel]) YIELD node
                RETURN node
                """, {"clusterId": cluster_id, "round": round, "newLabel": macro_vertex_label})
            tx.success = True
            tx.close()

round = 0
cluster_label = "Cluster"

nearest_neighbour("Token")
union_find("Token", round)
number_of_clusters = check_clusters(round)

while True:
    print("counter: {0}".format(round))
    macro_vertex_label = "MacroVertex%d" % round
    macro_vertex(macro_vertex_label, round)
    nearest_neighbour(macro_vertex_label)

    round += 1

    union_find(macro_vertex_label, round)
    number_of_clusters = check_clusters(round)

    if number_of_clusters == 1:
        sys.exit(1)
