-- 샘플: 단일 버전 실행 템플릿
-- 운영 플로우 생성 전, 수동 실행 시 날짜 배열만 교체해 사용.
select dm.refresh_dashboard_dm(
    p_version => '12410',
    p_character_dates => array[date '2025-12-24', date '2025-12-31'],
    p_agg_dates => array[date '2025-12-24', date '2025-12-31']
);
