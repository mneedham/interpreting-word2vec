from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo"))

with open("small_glove.txt", "r") as glove_file, driver.session() as session:
    rows = glove_file.readlines()

    params = []
    for row in rows:
        parts = row.split(" ")
        id = parts[0]
        embedding = [float(part) for part in parts[1:]]

        params.append({"id": id, "embedding": embedding})

    session.run("""\
    UNWIND {params} AS row
    MERGE (t:Token {id: row.id})
    ON CREATE SET t.embedding = row.embedding
    """, {"params": params})