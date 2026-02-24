-- 12410 버전 백필 실행 (12/18 업데이트 전후)
select dm.refresh_dashboard_dm(
    p_version => '12410',
    p_update_date => date '2025-12-18',
    p_character_dates => array[
        date '2025-12-24',
        date '2025-12-31'
    ],
    p_agg_dates => array[
        date '2025-12-10',
        date '2025-12-17',
        date '2025-12-24',
        date '2025-12-31'
    ]
);
