CREATE VECTOR INDEX {labelName}_chunk_embedding IF NOT EXISTS
FOR (c:{labelName}) 
ON (c.embedding)
OPTIONS {
 indexConfig: {
    `vector.dimensions`: {dimensions},
    `vector.similarity_function`: 'cosine'
 } 
}
