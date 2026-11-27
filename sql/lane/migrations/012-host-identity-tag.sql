-- PKG-468: every agent principal carries the identity tag `host:<its host>`, so a ticket whose requires_tags names it (`lane run --runner <host>`) is claimable only by that
-- runner. It is kept in principals.tags itself, the one list eligible(), scarcity() and ld_claim already read, so no claim function changes and an older runner is
-- filtered by the same SQL. Any other `host:` tag an admin sets is dropped: the prefix belongs to this trigger.
CREATE FUNCTION lane_dispatch.principal_host_tag() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  IF NEW.kind = 'agent' THEN
    NEW.tags := coalesce((SELECT array_agg(t ORDER BY o) FROM unnest(NEW.tags) WITH ORDINALITY AS u(t, o) WHERE t NOT LIKE 'host:%'), '{}') || ARRAY['host:' || NEW.host];
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER principals_host_tag BEFORE INSERT OR UPDATE OF tags, host, kind ON lane_dispatch.principals
  FOR EACH ROW EXECUTE FUNCTION lane_dispatch.principal_host_tag();

UPDATE lane_dispatch.principals SET tags = tags WHERE kind = 'agent';
