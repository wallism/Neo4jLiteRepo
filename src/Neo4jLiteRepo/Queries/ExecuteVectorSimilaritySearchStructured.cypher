// Structured vector similarity search returning flat rows for easier downstream processing
// Parameters: $questionEmbedding (list<float>), $topK (int), $similarityThreshold (float)
// Result columns: chunkId, articleTitle, articleUrl, snippetType, content, baseScore, sequence, entities
// NOTE: Some legacy data may have sequence stored as a string. We defensively coerce with toInteger() wherever
// numeric operations or ordering are required to avoid type errors.

// 1. Base semantic matches
MATCH (c:ContentChunk)
WHERE c.embedding IS NOT NULL
 AND size(c.embedding) = size($questionEmbedding)
WITH c, vector.similarity.cosine(c.embedding, $questionEmbedding) AS baseScore
WHERE baseScore >= $similarityThreshold
ORDER BY baseScore DESC
LIMIT $topK

// 2. Resolve owning article and any referenced entities
MATCH (a:Article)-[:HAS_SECTION|HAS_SUB_SECTION|CONTAINS*1..4]->(c)
OPTIONAL MATCH (c)-[:REFERENCES]->(e:Entity)
WITH c, a, baseScore, collect(DISTINCT e.name) AS entities

// 3. Emit main chunk row
RETURN c.id AS chunkId,
       a.title AS articleTitle,
       a.documentUrl AS articleUrl,
       'main' AS snippetType,
       c.content AS content,
       baseScore AS baseScore,
       COALESCE(toInteger(c.sequence),0) AS sequence,
       entities AS entities
UNION ALL
// 4. Context expansion: gather related chunks around each main chunk
MATCH (c:ContentChunk)
WHERE c.embedding IS NOT NULL
 AND size(c.embedding) = size($questionEmbedding)
WITH c, vector.similarity.cosine(c.embedding, $questionEmbedding) AS baseScore
WHERE baseScore >= $similarityThreshold
ORDER BY baseScore DESC
LIMIT $topK
MATCH (a:Article)-[:HAS_SECTION|HAS_SUB_SECTION|CONTAINS*1..4]->(c)
WITH c, a, baseScore
// NEXT forward
OPTIONAL MATCH (c)-[:NEXT*1..2]->(nextChunk:ContentChunk)
// NEXT backward
OPTIONAL MATCH (prevChunk:ContentChunk)-[:NEXT*1..2]->(c)
// Sequence siblings (within ±2)
OPTIONAL MATCH (sibling:ContentChunk)
WHERE sibling.parentId = c.parentId
    AND sibling.id <> c.id
    AND abs( coalesce(toInteger(sibling.sequence),0) - coalesce(toInteger(c.sequence),0) ) <= 2 // handle string/int sequences
// Section & SubSection scoped
WITH c,a,baseScore,nextChunk,prevChunk,sibling,
     c.parentType AS parentType, c.parentId AS parentId
OPTIONAL MATCH (sectionParent:Section {id: parentId})-[:CONTAINS]->(sectionChunk:ContentChunk)
WHERE parentType = 'Section' AND sectionChunk.id <> c.id
OPTIONAL MATCH (subSectionParent:SubSection {id: parentId})-[:CONTAINS]->(subSectionChunk:ContentChunk)
WHERE parentType = 'SubSection' AND subSectionChunk.id <> c.id
WITH c,a,baseScore,
     COLLECT(DISTINCT nextChunk) AS nextChunks,
     COLLECT(DISTINCT prevChunk) AS prevChunks,
     COLLECT(DISTINCT sibling) AS siblingChunks,
     COLLECT(DISTINCT sectionChunk) AS sectionChunks,
     COLLECT(DISTINCT subSectionChunk) AS subSectionChunks
UNWIND nextChunks + prevChunks + siblingChunks + sectionChunks + subSectionChunks AS ctx
WITH c,a,baseScore,ctx,
     CASE
        WHEN ctx IN nextChunks THEN 'next'
        WHEN ctx IN prevChunks THEN 'previous'
        WHEN ctx IN siblingChunks THEN 'sibling'
        WHEN ctx IN sectionChunks THEN 'section_related'
        WHEN ctx IN subSectionChunks THEN 'subsection_related'
        ELSE 'other'
     END AS snippetType
WHERE ctx IS NOT NULL
// Distinct rows by ctx.id + type (UNION ALL ensures mains kept separate)
RETURN ctx.id AS chunkId,
       a.title AS articleTitle,
       a.documentUrl AS articleUrl,
       snippetType AS snippetType,
       ctx.content AS content,
       baseScore * CASE // attenuate context scores
           WHEN snippetType = 'next' OR snippetType = 'previous' THEN 0.3
           WHEN snippetType = 'sibling' THEN 0.3
           WHEN snippetType = 'section_related' THEN 0.25
           WHEN snippetType = 'subsection_related' THEN 0.25
           ELSE 0.2
       END AS baseScore,
       COALESCE(toInteger(ctx.sequence),0) AS sequence,
       [] AS entities;