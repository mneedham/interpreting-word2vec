MATCH path = (:Token {id: "sons"})<-[:CONTAINS]-()-[:CONTAINS]->(sibling)
RETURN path