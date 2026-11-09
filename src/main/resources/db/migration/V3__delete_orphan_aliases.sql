-- Deletes the aliases of datasets that no longer exist. Before 2.0, deleting a dataset left its aliases behind, so
-- that they stayed reserved and could not be set on another dataset; deleting a dataset now deletes its aliases in
-- the same transaction (DOM-2.04). Also runs on databases of 1.x, which are baselined at version 0.
DELETE FROM aliases a WHERE NOT EXISTS (SELECT 1 FROM datasets d WHERE d.id = a.dataset_id);
