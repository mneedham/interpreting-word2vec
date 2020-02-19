from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo"), encrypted=False)

with open("/data/medium_glove.txt", "r", encoding='utf-8') as glove_file, driver.session() as session:
    rows = glove_file.readlines()

    params = []
    for row in rows:
        parts = row.split(" ")
        id = parts[0]
        embedding = [float(part) for part in parts[1:]]

        params.append({"id": id, "embedding": embedding})

    session.run("""\
    UNWIND $params AS row
    MERGE (t:Token {id: row.id})
    ON CREATE SET t.embedding = row.embedding
    """, {"params": params})

    session.run("""\
    CREATE CONSTRAINT ON (c:Cluster)
    ASSERT (c.id, c.round) IS NODE KEY""")

    session.run("""\
    CREATE CONSTRAINT ON (t:Token)
    ASSERT t.id IS UNIQUE""")

    session.run("""\
    CREATE INDEX ON :Cluster(round)""")
