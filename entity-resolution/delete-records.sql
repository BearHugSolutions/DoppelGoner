BEGIN;

-- Store the IDs of the entities to be deleted into a temporary table
CREATE TEMPORARY TABLE entities_to_delete AS
SELECT id
FROM public.entity -- Explicitly use public.entity
WHERE source_system = 'Snowflake - feature not fully implemented in pipeline';

-- Delete records from public.entity_edge_visualization that directly reference the problematic entities
DELETE FROM public.entity_edge_visualization
WHERE entity_id_1 IN (SELECT id FROM entities_to_delete)
OR entity_id_2 IN (SELECT id FROM entities_to_delete);

-- Delete records from public.entity_group that directly reference the problematic entities
DELETE FROM public.entity_group
WHERE entity_id_1 IN (SELECT id FROM entities_to_delete)
OR entity_id_2 IN (SELECT id FROM entities_to_delete);

-- Delete records from public.entity_feature that directly reference the problematic entities
DELETE FROM public.entity_feature
WHERE entity_id IN (SELECT id FROM entities_to_delete);

-- NEW: Delete records from clustering_metadata.entity_context_features that directly reference the problematic entities
DELETE FROM clustering_metadata.entity_context_features
WHERE entity_id IN (SELECT id FROM entities_to_delete);

-- Finally, delete the problematic entity records themselves
DELETE FROM public.entity -- Explicitly use public.entity
WHERE id IN (SELECT id FROM entities_to_delete);

-- Drop the temporary table
DROP TABLE entities_to_delete;

-- If everything looks correct, commit the transaction. Otherwise, ROLLBACK.
COMMIT;
-- ROLLBACK; -- Keeping ROLLBACK here for safety until you confirm