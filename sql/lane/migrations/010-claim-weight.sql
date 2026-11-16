-- BRAIN-1106: no table changes. ld_claim takes the weight the runner's broker can admit, and ld_finish requeues a ticket whose supervisor died unstarted; the version row marks a queue that has both.
SELECT 1;
