아래는 네가 말한 **세그먼트 정의(중위권=50~69, 상위권=90+ 또는 n<15면 80+)**를 반영해서, **Outcome(무릉 참여·성과) / Stat / Build** 3개 KPI와 **직업×세그먼트 순위용 Total Shift**를 “간결한 공식”으로 정리한 버전이다. 마지막에 사용되는 통계 기법(표준화, JSD, HHI, 로그비율, RMS, 표본수 보정)을 구체적으로 설명한다.

---

## 0) 표기와 집계 단위

* 버전: (v), 직업: (j), 세그먼트: (s \in {\text{mid}, \text{top}})

* **버전 집계는 count 합산 기반** 권장
  (\text{job_count}*{j,s,v} = \sum*{t \in v}\text{job_count}*{j,s,t})
  (\text{total}*{s,v} = \sum_{t \in v}\text{total}_{s,t})

* **변화량은 인접 버전 차이**: (\Delta x_{j,s,v} = x_{j,s,v} - x_{j,s,v-1})

* 표본수가 작은 상위권(top)은 **n 보정 가중치**를 둔다:
  (g(n)=\sqrt{\frac{n}{n+k}}) (예: (k=20))
  → (n) 작을수록 변동이 과대평가되는 걸 완화

---

# 1) Outcome Shift (무릉 참여·성과) — 세그먼트 반영

네 말대로:

* **중위권(mid)**: 세그먼트 내부는 50~69층이라 “50층 달성률”이 정의상 100%에 가까워짐 → **직업 전체 50층 달성률**을 Outcome에 넣어야 의미가 생김
* **상위권(top)**: Outcome은 “얼마나 올라갔나”가 핵심 → **최고층수(또는 상위퍼센타일 층수)**가 적합

또한 **상단 KPI 카드에는 mid/top 둘 다 반영**해야 함.

---

## 1-1) 공통 구성요소

### (A) 참여율(점유율) — count 기반

[
\text{share}*{j,s,v}=\frac{\text{job_count}*{j,s,v}}{\text{total}*{s,v}}
]
변화는 **로그비율**(log-ratio)로:
[
\Delta \text{share}^{\log}*{j,s,v}=\log\frac{\text{share}*{j,s,v}+\epsilon}{\text{share}*{j,s,v-1}+\epsilon}
]
((\epsilon)은 0 방지용 작은 값)

### (B) 속도/효율(선택)

[
\text{sec_per_floor}*{j,s,v}=\text{median}*{\text{ch}\in(j,s,v)}(\text{sec/floor})
]
[
\Delta \text{eff}*{j,s,v}= -\Delta \text{sec_per_floor}*{j,s,v}
]
(낮을수록 좋으니 부호 반전)

---

## 1-2) 중위권(mid) Outcome: “직업 전체 50층 달성률” 포함

직업 전체(세그먼트 무관) 50층 달성률:
[
\text{clear50}*{j,v}=
\frac{#(\text{ch of job }j \text{ with floor}\ge 50 \text{ in }v)}
{#(\text{all ch of job }j \text{ in }v)}
]
[
\Delta \text{clear50}*{j,v}=\text{clear50}*{j,v}-\text{clear50}*{j,v-1}
]

**중위권 OutcomeShift**
[
\boxed{
O_{j,\text{mid},v}
==================

z(\Delta \text{share}^{\log}*{j,\text{mid},v})
+
z(\Delta \text{clear50}*{j,v})
+
z(\Delta \text{eff}_{j,\text{mid},v})
}
]

---

## 1-3) 상위권(top) Outcome: “최고층수(견고한 지표로)” 포함

최고층수는 outlier에 민감하므로 **max 대신 상위 퍼센타일 권장**:
[
\text{topFloor}*{j,\text{top},v}=\text{quantile}*{0.95}(\text{floor})
]
[
\Delta \text{topFloor}*{j,\text{top},v}=\text{topFloor}*{j,\text{top},v}-\text{topFloor}_{j,\text{top},v-1}
]

**상위권 OutcomeShift (+ 표본수 보정)**
[
\boxed{
O_{j,\text{top},v}
==================

g(n_{j,\text{top},v})\cdot
\Big(
z(\Delta \text{share}^{\log}*{j,\text{top},v})
+
z(\Delta \text{topFloor}*{j,\text{top},v})
+
z(\Delta \text{eff}_{j,\text{top},v})
\Big)
}
]

---

## 1-4) 상단 KPI(Outcome Volatility): mid/top 둘 다 포함

직업별 절댓값 이동량을 평균하거나, 변동성(RMS)로:

[
\boxed{
\text{KPI_Outcome}_v
====================

\sqrt{
\frac{1}{|J|}
\sum_{j\in J}
\Big(
\alpha , O_{j,\text{mid},v}^2
+
(1-\alpha), O_{j,\text{top},v}^2
\Big)
}
}
]

* (\alpha)는 mid/top 반영 비율(예: 0.6~0.7)
* RMS를 쓰면 “몇몇 직업만 크게 움직인 경우”도 잘 잡음

---

# 2) Stat Shift (헥사코어/어빌/하이퍼)

핵심은 “수치 변화” + “선택 분포 변화”를 분리하는 것.

* 헥사: **pick 분포 변화(JSD)** + **level_sum 변화(z)**
* 어빌/하이퍼: **분포 변화(JSD)** 중심

직업×세그먼트×버전에서:

* 헥사코어 분포 (p^{\text{hexa}}_{j,s,v}) (core별 pick_count/총합)
* 어빌 분포 (p^{\text{abil}}_{j,s,v}) (항목별 count/총합)
* 하이퍼 분포 (p^{\text{hyper}}_{j,s,v}) (stat별 count/총합 혹은 top3 집계 기반)

[
\boxed{
S_{j,s,v}
=========

\text{JSD}!\left(p^{\text{hexa}}*{j,s,v},p^{\text{hexa}}*{j,s,v-1}\right)
+
z!\left(|\Delta \text{hexa_level_sum}*{j,s,v}|\right)
+
\text{JSD}!\left(p^{\text{abil}}*{j,s,v},p^{\text{abil}}*{j,s,v-1}\right)
+
\text{JSD}!\left(p^{\text{hyper}}*{j,s,v},p^{\text{hyper}}_{j,s,v-1}\right)
}
]

---

# 3) Build Shift (스타포스/세트/무기)

* 스타포스: 수치 변화(z)
* 세트/무기: 선택 분포 변화(JSD) + 주류 교체(Switch) 보조

[
\boxed{
B_{j,s,v}
=========

z(|\Delta \text{starforce}*{j,s,v}|)
+
\text{JSD}(p^{\text{weapon}}*{j,s,v},p^{\text{weapon}}*{j,s,v-1})
+
\text{JSD}(p^{\text{set}}*{j,s,v},p^{\text{set}}*{j,s,v-1})
+
\lambda\cdot \text{TopKSwitch}*{j,s,v}
}
]

* TopKSwitch: 전/현 버전 Top-3 항목 중 교체 비율(0~1)
* (\lambda): 스위치 항목의 가중(예: 0.2~0.4)

---

# 4) 직업×세그먼트 최종 Total Shift (랭킹용)

## 4-1) 카테고리 간 “균등합산” 대신 표준화 + 가중

각 카테고리 점수는 스케일이 다르므로, 먼저 직업 집합 기준으로 z-score:

[
Z_O = z(O_{j,s,v}),\quad Z_S=z(S_{j,s,v}),\quad Z_B=z(B_{j,s,v})
]

### 권장 가중치(메이플 해석 기준)

* mid: Outcome 비중↑ (성과/참여가 메타 반응에 더 직접)
* top: Stat/Build 비중↑ (코어/세팅이 메타를 정의)

[
\boxed{
T_{j,\text{mid},v}=0.5 Z_O+0.3 Z_S+0.2 Z_B
}
]
[
\boxed{
T_{j,\text{top},v}=0.3 Z_O+0.35 Z_S+0.35 Z_B
}
]

랭킹은 “이동량”을 보려면:
[
\boxed{\text{Rank by } |T_{j,s,v}|}
]
방향(버프/너프 추정 신호)은 부호로:
[
\text{direction}=\text{sign}(T_{j,s,v})
]

---

# 5) 사용된 통계 기법 설명(구체)

## (1) 로그비율(Log-ratio) — 점유율 변화에 적합

(\Delta \text{share})는 작은 직업에서 과소평가되기 쉬움.
로그비율은 “상대 변화”를 선형화한다.

* 0.5% → 1.0% 는 **+100%**로 큰 변화
* 10% → 10.5% 는 **+5%**로 상대 변화는 작음
  메타 관점에선 전자가 더 큰 사건이라 로그비율이 맞다.

## (2) z-score 표준화

[
z(x)=\frac{x-\mu}{\sigma}
]
카테고리/지표마다 단위가 다르기 때문에 “공정한 합산”을 위해 필수.
직업들 사이의 평균/표준편차로 스케일을 맞춘다.

## (3) JSD(Jensen–Shannon Divergence) — 선택 분포 변화량

세트/무기/어빌/헥사코어처럼 “카테고리 선택”은 분포로 표현된다.
JSD는 두 분포 간 거리로:

* 0이면 변화 없음
* 값이 커질수록 “선택 메타가 재편”됨
  KL divergence보다 안정적이며(대칭·유한), 실제 운영/대시보드에서 해석이 쉽다.

## (4) HHI/집중도(선택적 보조)

[
\text{HHI}=\sum_i p_i^2
]

* 높아지면 “한두 개로 쏠림(메타 경직)”
* 낮아지면 “다양화(혼란/실험)”
  KPI 설명 텍스트로 붙이기 좋고, JSD 보조로 유용.

## (5) 상위권 표본수 보정 (g(n))

상위권은 직업별 샘플 수가 적고(특히 n<15 조건),
단 1~2명의 변동이 분포/최고층을 크게 흔들 수 있음.
(g(n)=\sqrt{\frac{n}{n+k}}) 같은 shrinkage는

* n이 작을수록 점수를 줄여 과대해석을 막고
* n이 충분하면 거의 1에 수렴해 원래 신호를 보존한다.

## (6) RMS(제곱평균제곱근) KPI

[
\sqrt{\text{mean}(x^2)}
]
평균 절댓값보다 “극단적 변동”에 민감하다.
패치로 특정 직업군이 크게 흔들리면 KPI가 확실히 반응한다.

---

## 6) 상단 KPI 카드 문구까지 깔끔히 맞추는 방식(권장)

* **Outcome Volatility**: 참여율(로그비율) + (mid: 전체 50층 달성률) + (top: 상위퍼센타일 최고층) + 효율
* **Stat Volatility**: 헥사코어(픽 분포+레벨) + 어빌 + 하이퍼
* **Build Volatility**: 스타포스 + 무기 + 세트효과(+보조무기 포함)

각 KPI는 위 식대로 RMS로 산출.

---

원하면, 너의 **세그먼트 규칙(90+, n<15이면 80+)**을 그대로 쓰되 “topFloor=0.95 quantile”이 과한지(샘플 작을 때는 0.9가 더 안정적)까지 포함해서, **권장 퍼센타일 선택 로직**도 같이 제안해줄게.
