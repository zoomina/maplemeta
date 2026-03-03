-- 1) 2026-02-25 데이터 version 12412로 변경
-- PK에 version 포함 → UPDATE로 변경 (기존 잘못된 version 12410 등 → 12412)

update dm.dm_rank set version = '12412' where date = '2026-02-25' and version != '12412';
update dm.dm_force set version = '12412' where date = '2026-02-25' and version != '12412';
update dm.dm_hyper set version = '12412' where date = '2026-02-25' and version != '12412';
update dm.dm_ability set version = '12412' where date = '2026-02-25' and version != '12412';
update dm.dm_seedring set version = '12412' where date = '2026-02-25' and version != '12412';
update dm.dm_equipment set version = '12412' where date = '2026-02-25' and version != '12412';
update dm.hyper_master set version = '12412' where date = '2026-02-25' and version != '12412';
update dm.dm_hexacore set version = '12412' where date = '2026-02-25' and version != '12412';

-- 2) version_master: end_date null → today
update dm.version_master
set end_date = current_date
where end_date is null;
