// First find the most semantically relevant chunks
MATCH (c:ContentChunk)
WHERE c.embedding IS NOT NULL
WITH c, vector.similarity.cosine(c.embedding, $questionEmbedding) AS score
WHERE score > 0.6
ORDER BY score DESC                        
LIMIT $topK

// Find the article for each chunk
MATCH (a:Article)-[:HAS_SECTION|HAS_SUB_SECTION|CONTAINS*1..4]->(c)
OPTIONAL MATCH (c)-[:REFERENCES]->(e:Entity)

// Collect main results
WITH c, a, score, collect(e.name) AS entities

// Return all results in a single query with UNION ALL
CALL (c, a, score, entities) { 
    // Return the main search result chunks with 'main' result type
    RETURN 
        c AS mainChunk,
        c.id AS id,
        c.content AS content,
        a.title AS articleTitle,
        a.documentUrl AS articleUrl,
        score AS mainScore,
        entities AS mainEntities,
        COALESCE(c.sequence, 0) AS sequence, 
        'main' AS resultType
    
    UNION
    
    // Get context for each main result
    WITH c, a, score, entities
    
    // Get context using NEXT relationship - chunks that come after (fixed to 2 chunks)
    OPTIONAL MATCH (c)-[:NEXT*1..2]->(nextChunk:ContentChunk)
    
    // Get context using NEXT relationship - chunks that come before (fixed to 2 chunks)
    OPTIONAL MATCH (prevChunk:ContentChunk)-[:NEXT*1..2]->(c)
    
    // Also get context using parentId and Sequence for chunks that might not have NEXT relationships
    // Get siblings within 2 positions in sequence
    OPTIONAL MATCH (siblingChunk:ContentChunk)
    WHERE siblingChunk.parentId = c.parentId
      AND siblingChunk.id <> c.id
      AND abs(siblingChunk.Sequence - c.Sequence) <= 2
    
    // For Section and SubSection parents, get parent and related chunks
    WITH c, a, score, entities, nextChunk, prevChunk, siblingChunk,
         c.parentType AS parentType, c.parentId AS parentId
    
    // Get Section parent and related chunks when parentType is Section
    OPTIONAL MATCH (sectionParent:Section {id: parentId})-[:CONTAINS]->(sectionChunk:ContentChunk)
    WHERE parentType = 'Section' AND sectionChunk.id <> c.id
    
    // Get SubSection parent and related chunks when parentType is SubSection
    OPTIONAL MATCH (subSectionParent:SubSection {id: parentId})-[:CONTAINS]->(subSectionChunk:ContentChunk)
    WHERE parentType = 'SubSection' AND subSectionChunk.id <> c.id
    
    // Combine all results
    WITH c, a, score, entities,
        COLLECT(DISTINCT nextChunk) AS nextChunks,
        COLLECT(DISTINCT prevChunk) AS prevChunks,
        COLLECT(DISTINCT siblingChunk) AS siblingChunks,
        COLLECT(DISTINCT sectionParent) AS sectionParents,
        COLLECT(DISTINCT sectionChunk) AS sectionChunks,
        COLLECT(DISTINCT subSectionParent) AS subSectionParents,
        COLLECT(DISTINCT subSectionChunk) AS subSectionChunks
    
    // Unwind all collected chunks to prepare for deduplication 
    // and then add main result chunk to the collection
    UNWIND nextChunks + prevChunks + siblingChunks + sectionChunks + subSectionChunks AS contextChunk
    WITH contextChunk, c, a, nextChunks, prevChunks, siblingChunks, sectionChunks, subSectionChunks
    WHERE contextChunk IS NOT NULL
    
    // Group by chunk id to get highest score per chunk and deduplicate
    WITH contextChunk,
        contextChunk.id AS id,
        contextChunk.content AS content,
        a.title AS articleTitle,
        a.documentUrl AS articleUrl,
        MAX(CASE 
            WHEN contextChunk IN nextChunks THEN 0.3
            WHEN contextChunk IN prevChunks THEN 0.3
            WHEN contextChunk IN siblingChunks THEN 0.3
            WHEN contextChunk IN sectionChunks THEN 0.25
            WHEN contextChunk IN subSectionChunks THEN 0.25
            ELSE 0.2
        END) AS contextScore,
        [] AS contextEntities,
        MIN(COALESCE(contextChunk.Sequence, 0)) AS sequence,
        head(collect(CASE 
            WHEN contextChunk IN nextChunks THEN 'next'
            WHEN contextChunk IN prevChunks THEN 'previous'
            WHEN contextChunk IN siblingChunks THEN 'sibling'
            WHEN contextChunk IN sectionChunks THEN 'section_related'
            WHEN contextChunk IN subSectionChunks THEN 'subsection_related'
            ELSE 'other'    
        END)) AS resultType
    WHERE id IS NOT NULL
    ORDER BY contextScore DESC
    
    RETURN 
        contextChunk AS mainChunk,
        id,
        content, 
        articleTitle, 
        articleUrl, 
        contextScore AS mainScore, 
        contextEntities AS mainEntities, 
        COALESCE(sequence, 0) AS sequence, 
        resultType
}

RETURN *