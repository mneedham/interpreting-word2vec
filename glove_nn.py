import sys
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo"))


def nearest_neighbour(label):
    with driver.session() as session:
        result = session.run("""\
        MATCH (t:`%s`)
        MATCH (other:`%s`)
        WHERE other <> t
        WITH t, collect({token: other, distance: apoc.algo.euclideanDistance(t.embedding, other.embedding)}) AS others
        WITH t, apoc.coll.sortMaps(others, "distance")[0] AS closest
        WITH t AS token, closest.token AS closest, closest.distance AS distance
        MERGE (token)-[nearest:NEAREST_TO]->(closest)
        ON CREATE SET nearest.weight = distance
        """ % (label, label))
        print(result.summary().counters)


def union_find(label, cluster_label):
    with driver.session() as session:
        result = session.run("""\
        CALL algo.unionFind.stream({label}, "NEAREST_TO")
        YIELD nodeId, setId
        MATCH (token) WHERE id(token) = nodeId
        MERGE (cluster:`%s` {id: setId })
        MERGE (cluster)-[:CONTAINS]->(token)
        """ % cluster_label, {"label": label})
        print(result.summary().counters)


def check_clusters(cluster_label):
    with driver.session() as session:
        return session.run("MATCH (n:`%s`) RETURN count(*) AS clusters" % cluster_label).peek()["clusters"]


def macro_vertex(cluster_label, macro_vertex_label):
    with driver.session() as session:
        result = session.run("""\
        MATCH (cluster:`%s`)
        RETURN cluster
        """ % cluster_label)

        for row in result:
            cluster_id = row["cluster"]["id"]

            session.run("""\
            MATCH (cluster:`%s` {id: {clusterId} })-[:CONTAINS]->(token)
            WITH cluster, collect(token) AS tokens
            UNWIND tokens AS t1 UNWIND tokens AS t2 WITH t1, t2 WHERE t1 <> t2
            WITH  t1, reduce(acc = 0, t2 in collect(t2) | acc + apoc.algo.euclideanDistance(t1.embedding, t2.embedding)) AS distance
            WITH t1, distance ORDER BY distance LIMIT 1
            CALL apoc.create.addLabels(t1, [{newLabel}]) YIELD node
            RETURN node
            """ % cluster_label, {"clusterId": cluster_id, "newLabel": macro_vertex_label})


nearest_neighbour("Token")
union_find("Token", "Cluster")
number_of_clusters = check_clusters("Cluster")

cluster_label = "Cluster"
counter = 1
while True:
    macro_vertex_label = "MacroVertex%d" % counter
    macro_vertex(cluster_label, macro_vertex_label)
    nearest_neighbour(macro_vertex_label)

    cluster_label = "Cluster-MV%s" % counter

    union_find(macro_vertex_label, cluster_label)
    number_of_clusters = check_clusters(cluster_label)

    if number_of_clusters == 1:
        sys.exit(1)
    else:
        counter = counter + 1
