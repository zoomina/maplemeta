-- 12409/12410 버전 백필 실행 (날짜별 버전 분리)
select dm.refresh_dashboard_dm(
    p_version => '12409',
    p_character_dates => array[
        date '2025-12-10',
        date '2025-12-17'
    ],
    p_agg_dates => array[
        date '2025-12-10',
        date '2025-12-17'
    ]
);

select dm.refresh_dashboard_dm(
    p_version => '12410',
    p_character_dates => array[
        date '2025-12-24',
        date '2025-12-31'
    ],
    p_agg_dates => array[
        date '2025-12-24',
        date '2025-12-31'
    ]
);
