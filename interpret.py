from sklearn.neighbors import NearestNeighbors
import numpy as np

from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo"))

X = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9], [1, 2, 4]])
nbrs = NearestNeighbors(n_neighbors=2, algorithm='ball_tree').fit(X)
distances, indices = nbrs.kneighbors(X)

adjacency_matrix = nbrs.kneighbors_graph(X).toarray()

with driver.session() as session:
    for row_index, row in enumerate(adjacency_matrix):
        for col_index, col in enumerate(row):
            if row_index != col_index and col == 1:
                print(row_index, col_index, col, float(distances[row_index][1]))

                params = {
                    "node1": row_index,
                    "node2": col_index,
                    "weight": float(distances[row_index][1])
                }

                # builds nearest neighbour graph
                session.run("""\
                MERGE (n1:Node {id: {node1} })
                MERGE (n2:Node {id: {node2} })
                MERGE (n1)-[nearest:NEAREST_TO]->(n2)
                SET nearest.weight = {weight}
                """, params)

NNG = "our initial graph of embeddings"

for iteration in range(0, 6):
    print("run connected components algorithm over NNG")
    print("create macro vertices based on connect components output")
    NNG = "a graph of the latest set of macro vertices"
