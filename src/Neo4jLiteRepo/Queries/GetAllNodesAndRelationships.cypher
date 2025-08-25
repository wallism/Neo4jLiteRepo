CALL db.labels() YIELD label AS nodeType
WITH nodeType
ORDER BY nodeType
// Find outgoing relationships
OPTIONAL MATCH (n)-[r]->() 
WHERE nodeType IN labels(n)
WITH nodeType, collect(DISTINCT type(r)) AS allOutgoingRels
// Find incoming relationships
OPTIONAL MATCH (n)<-[r]-() 
WHERE nodeType IN labels(n)
WITH nodeType, allOutgoingRels, collect(DISTINCT type(r)) AS allIncomingRels
// Filter out excluded relationships
WITH 
    nodeType,
    [rel IN allOutgoingRels WHERE rel IS NOT NULL AND NOT rel IN $excludedOutRels ] AS outgoingRels,
    [rel IN allIncomingRels WHERE rel IS NOT NULL AND NOT rel IN $excludedInRels] AS incomingRels
// Format results
RETURN 
    nodeType AS NodeType,
    outgoingRels AS OutgoingRelationships,
    incomingRels AS IncomingRelationships
ORDER BY nodeType
