-- 2026-02-25 데이터 DM 백필
-- version: 12412 (최신, end_date null = open-ended)
-- 실행 전: DW에 26-02-25 character_info 완료 여부 확인 권장

-- 1) refresh_dashboard_dm
select dm.refresh_dashboard_dm(
    p_version => '12412',
    p_character_dates => array[date '2026-02-25'],
    p_agg_dates => array[date '2026-02-25']
);

-- 2) shift_score 갱신
select dm.refresh_shift_balance_score('12412');
