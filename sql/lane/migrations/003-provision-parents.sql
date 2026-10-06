-- BRAIN-400 round 2 (N4): scheduling parents are provisioned with the account, so submit_group only READS its parent and never takes
-- the scheduling lock to create one. A parent is (account, class) and there are a handful of classes, so every principal's accounts
-- get one parent per class when the principal is created or gains an account, and every account gets one when a class is added.
-- Backfilled here for principals that already exist.
CREATE FUNCTION lane.provision_account_parents() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
BEGIN
  INSERT INTO lane.sched_parents (account, prio_class)
    SELECT a, c.name FROM unnest(NEW.allowed_accounts) AS a CROSS JOIN lane.priority_classes c
    ON CONFLICT (account, prio_class) DO NOTHING;
  RETURN NULL;
END $$;

CREATE FUNCTION lane.provision_class_parents() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
BEGIN
  INSERT INTO lane.sched_parents (account, prio_class)
    SELECT DISTINCT a, NEW.name FROM lane.principals p, unnest(p.allowed_accounts) AS a
    ON CONFLICT (account, prio_class) DO NOTHING;
  RETURN NULL;
END $$;

CREATE TRIGGER principals_provision_parents AFTER INSERT OR UPDATE OF allowed_accounts ON lane.principals
  FOR EACH ROW EXECUTE FUNCTION lane.provision_account_parents();
CREATE TRIGGER classes_provision_parents AFTER INSERT ON lane.priority_classes
  FOR EACH ROW EXECUTE FUNCTION lane.provision_class_parents();

INSERT INTO lane.sched_parents (account, prio_class)
  SELECT DISTINCT a, c.name FROM lane.principals p, unnest(p.allowed_accounts) AS a, lane.priority_classes c
  ON CONFLICT (account, prio_class) DO NOTHING;
