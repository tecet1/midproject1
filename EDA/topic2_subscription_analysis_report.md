# Topic2 구독 전환 및 리텐션 분석 방향성 보고서

## 1. 목적과 전제

이 플랫폼은 기본적으로 유료 구독 기반 콘텐츠 플랫폼이다. 따라서 핵심 분석 질문은 단순 클릭 분석이 아니라 아래 두 축으로 나뉜다.

- 구독 라이프사이클 관점: 첫 구독 이후 유지와 복귀가 어떻게 일어나는가
- 콘텐츠 페이지 CTA 관점: 어떤 버튼/문구/가격 조건이 신규 구독 또는 재구독으로 이어지는가

구독 관련 테이블의 의미는 다음과 같이 정리한다.

- `complete_subscription`: 첫 구독 이벤트
- `renew_subscription`: 구독 단위 기간 갱신 이벤트
- `resubscribe_subscription`: 구독 종료 후 다시 시작한 재구독 이벤트
- `start_free_trial`: 무료 체험 시작 이벤트

이 정의를 기준으로 보면, `renew`는 리텐션/라이프사이클 분석용 지표이고, CTA의 직접 전환 endpoint로 쓰기에는 부적절하다. 반면 `complete_subscription`은 신규 전환, `resubscribe_subscription`은 재획득 전환 endpoint로 해석하는 것이 자연스럽다.

### 1-1. 왜 AARRR 전체 프레임을 쓰지 않는가

이번 프로젝트는 AARRR 전체 단계를 빠짐없이 채우는 분석보다, 현재 데이터가 잘 설명할 수 있는 질문에 집중하는 편이 더 적절하다.

그 이유는 아래와 같다.

- 현재 데이터의 중심은 구독 이벤트와 콘텐츠 페이지 행동이다.
- `Referral`은 직접적으로 측정할 수 있는 추천/초대 데이터가 없다.
- `Acquisition`도 마케팅 채널 데이터가 없어 제한적이다.
- `Activation` 역시 일반적인 서비스의 활성화보다, 이 플랫폼에서는 구독/체험 진입과 실제 결제가 더 중요하다.

즉, 이번 프로젝트는 AARRR을 억지로 모두 채우는 것보다 아래 세 축에 집중하는 것이 더 논리적이다.

- 구독 전환
- 구독 리텐션
- 재구독/복귀

필요 시 보조 축으로 아래를 둔다.

- 무료 체험 경유 여부
- 전환 속도
- realized revenue

### 1-2. 왜 파생변수를 최소한으로만 생성하는가

이번 분석은 "질문에 답하기 위해 꼭 필요한 변수"만 만드는 것이 좋다.

파생변수를 과도하게 늘리면 아래 문제가 생긴다.

- 해석 복잡도가 불필요하게 올라간다.
- 같은 의미의 지표가 중복되어 결론이 흐려질 수 있다.
- 노트북과 결과표가 장황해져 핵심 메시지가 약해진다.

따라서 이번 프로젝트에서는 아래 수준의 최소 파생변수만 유지하는 것이 적절하다.

- cohort month
- 버튼 클릭 시점 기준 user state
- attribution window 내 전환 여부
- 플랜 길이/가격/할인 그룹
- time-to-convert
- voluntary churn proxy 여부

즉, 이 프로젝트는 "많이 만드는 분석"보다 "정확히 필요한 것만 만드는 분석"이 더 잘 맞는다.

## 2. 현재 전처리 방향 평가

현재 사용 중인 `preprocess_korea_data()` 전처리 방향은 전체적으로 타당하다.

- `scan_parquet` 기반 처리
- `Chrome Headless` 제거
- 문자열 결측치 표준화
- `client_event_time`의 KST 변환
- 가격 관련 컬럼 정수형 변환
- 완전 중복 제거

추가로 반드시 유지해야 할 원칙은 아래와 같다.

- 여러 테이블을 union 하거나 퍼널을 만들 때는 공통 관측 기간으로 잘라야 한다.
- 버튼명 비교는 버튼이 동시에 노출된 겹치는 기간에서만 해야 한다.
- `user_id` 기반 퍼널은 `user_id` 존재 로그만 별도 데이터프레임으로 만들어야 한다.
- 퍼널 연결 시 `하위 이벤트 시각 > 상위 이벤트 시각` 조건이 반드시 필요하다.

## 3. 현재 방향성에 대한 논리 점검

### 3-1. 리텐션 분석 방향

다음 방향은 논리적으로 매우 적절하다.

- `complete_subscription`, `renew_subscription`, `resubscribe_subscription`를 union
- `user_id`별로 timestamp 정렬
- `complete_subscription` 최초 시점을 기준으로 cohort month 정의
- 이후 월별로 union paid event가 다시 발생하는지 관찰

이 방식은 "첫 구독 이후 paid lifecycle이 얼마나 유지되는가"를 보는 데 적합하다.

즉, cohort의 기준점은 반드시 `complete_subscription`이어야 하고, 유지 여부는 `renew`와 `resubscribe`를 포함한 union paid event로 보는 것이 맞다.

### 3-2. CTA 전환 분석 방향

기존의 `content_page -> button click -> union subscription`은 넓게 보면 "결제 상태로 이어지는 paid event"를 보려는 방향이지만, 그대로 쓰면 신규 전환과 유지 이벤트가 섞인다.

따라서 CTA 분석에서는 endpoint를 분리해야 한다.

- 신규 전환 분석: `button click -> complete_subscription`
- 재획득 분석: `button click -> resubscribe_subscription`
- 리텐션 분석: `complete cohort -> renew/resubscribe`

즉, `renew`는 CTA 효과 검증용 endpoint가 아니라 retention/lifecycle 지표로 두는 것이 맞다.

## 4. 사용자가 제시한 세 가지 핵심 질문의 타당성

### 4-1. 버튼명별로 겹치는 기간에서 subscription 비율 차이가 있는가

타당한 질문이다. 다만 아래 조건이 붙어야 한다.

- 같은 기간에 동시에 노출된 버튼끼리만 비교
- 가능하면 같은 `content.id` 또는 유사 콘텐츠군 안에서 비교
- 같은 user state끼리 비교
- 같은 attribution window를 적용

여기서 user state는 최소한 아래처럼 구분해야 한다.

- 클릭 시점 이전 paid history 없음
- 클릭 시점 이전 paid history 있음

더 세밀하게는 아래처럼 나눌 수 있다.

- `never_subscribed`
- `lapsed_user`
- `active_or_historical_subscriber`

### 4-2. complete_subscription 또는 버튼명에 대응하는 케이스에서 구독 전환율 차이가 있는가

이 질문도 타당하다. 다만 endpoint를 둘로 나눠야 한다.

- 신규 유저 또는 미구독 유저 대상: `complete_subscription` 전환율
- 이탈 경험이 있는 유저 대상: `resubscribe_subscription` 전환율

같은 버튼이라도 누른 집단의 상태가 다르면 의미가 달라진다. 따라서 "어떤 유저에게 그 버튼이 노출되었는가"를 먼저 분리해야 한다.

### 4-3. plan.price와 paid_amount 차이를 고려해도 버튼명 효과가 남는가

이 질문은 매우 중요하다. CTA 효과처럼 보이는 차이가 사실상 가격/플랜/프로모션 차이일 수 있기 때문이다.

교란 요인은 대표적으로 아래와 같다.

- 1개월 vs 12개월 플랜 믹스 차이
- 할인 강도 차이
- 특정 프로모션 기간 효과
- 페이지 개편 시점 효과

즉, 버튼명과 전환율의 관계를 보려면 가격/할인/플랜 길이를 함께 통제해야 한다.

## 5. 추가 피드백 검토와 반영 방향

아래는 외부 조언 내용을 검토한 결과다.

### 5-1. 무료 체험 단계 분리

이 피드백은 타당하다.

현재 데이터에는 `start_free_trial`이 별도 존재하므로, 신규 전환 퍼널은 하나의 직선형 퍼널이 아니라 아래 두 경로를 함께 봐야 한다.

- 직접 결제 경로: `content_page -> button click -> complete_subscription`
- 체험 경로: `content_page -> button click -> start_free_trial -> complete_subscription`

중요한 점은 무료 체험이 있다고 해서 자동으로 `complete_subscription`이 비매출 이벤트가 되는 것은 아니라는 것이다. 현재 스키마상 `start_free_trial`과 `complete_subscription`이 분리되어 있으므로, 우선은 `complete_subscription`을 첫 유료 구독 endpoint로 해석하는 것이 자연스럽다. 다만 운영 정의상 0원 구독이 `complete_subscription`에 포함될 가능성이 있으면 반드시 별도 확인이 필요하다.

다만 현재 확인된 바에 따르면 `start_free_trial`은 2023년 4월까지만 관측되고, 그 이후 기간에는 집계되지 않는다. 이 이유가 실제 프로모션 종료인지, 수집/집계 누락인지는 명확하지 않다. 따라서 전체 기간을 일관되게 다루는 핵심 분석축에는 넣지 않는 것이 더 보수적이고 안전하다.

즉, 무료 체험 단계는 아래처럼 취급하는 것이 적절하다.

- 핵심 분석축: 제외
- 보조 탐색 분석: 2021-12 ~ 2023-04 한정 시 참고 가능

또한 현재 프로젝트의 중심 모수를 "일단 첫구독을 어떻게든 한 사람들"로 잡는다면, 분석 시작점은 `complete_subscription`이므로 `start_free_trial`은 필수 단계가 아니다. 이 경우 free trial은 첫구독 이전 경로 정보일 뿐이며, 리텐션과 재구독을 설명하는 핵심 모수 정의에는 포함되지 않아도 된다.

### 5-2. 자발적 이탈 vs 비자발적 이탈 구분

이 피드백은 개념적으로 타당하다. 다만 현재 데이터에서 관측 가능한 범위를 구분해야 한다.

현재 직접 관측 가능한 것은 아래 정도다.

- `click_cancel_plan_button`: 자발적 이탈 의도 또는 자발적 해지의 proxy

현재 직접 관측이 어려운 것은 아래다.

- 카드 한도 초과
- 카드 만료
- 결제 실패
- 청구 오류

즉, 현재 데이터로는 "자발적 이탈 proxy"는 볼 수 있지만, "비자발적 이탈"은 직접 식별할 수 없다. 따라서 보고서에는 아래처럼 쓰는 것이 맞다.

- 현재 데이터에서 가능한 분석: `cancel click -> resubscribe` 복귀 분석
- 현재 데이터에서 어려운 분석: involuntary churn 원인 분리

즉, 이 피드백은 전면 채택이 아니라 "데이터 한계 하의 부분 채택"이 맞다.

### 5-3. 통계적 유의성 검증 추가

이 피드백은 강하게 타당하다.

버튼명별 전환율 차이는 단순 비율 비교만으로는 판단하면 안 된다. 특히 표본 수가 다르거나 특정 버튼이 짧은 기간만 노출된 경우 착시가 생기기 쉽다.

따라서 아래 단계를 추가해야 한다.

- 카이제곱 검정 또는 two-proportion z-test
- p-value 확인
- 전환율 차이의 신뢰구간 계산
- 다중 비교 시 보정
- minimum sample threshold 설정

즉, "비율 차이가 있다"와 "의미 있는 차이다"는 분리해서 다뤄야 한다.

### 5-4. LTV/누적 매출 관점 검증

이 피드백도 타당하다.

특정 버튼이나 프로모션이 `complete_subscription` 전환율은 높일 수 있어도, 장기 가치가 낮다면 좋은 전략이라고 보기 어렵다.

다만 표현은 약간 조정하는 것이 좋다.

- 현재 분석에서 계산 가능한 것은 "관측 기간 내 realized revenue"이다.
- 진짜 의미의 완전한 lifetime value는 더 긴 관측 기간과 censoring 고려가 필요하다.

따라서 현재 프로젝트에서는 아래처럼 표현하는 것이 적절하다.

- 90일 또는 180일 누적 매출
- cohort별 realized ARPU
- button/promo cohort별 누적 `paid_amount`

즉, "LTV"라는 표현은 쓰되, 보고서에서는 `LTV proxy` 또는 `realized LTV`로 명시하는 것이 더 정확하다.

### 5-5. Time-to-Convert 분석

이 피드백도 매우 타당하다.

같은 전환율이라도 전환 속도는 다를 수 있다. 예를 들어 할인형 버튼은 당일 결제가 많고, 프리미엄 가치형 버튼은 며칠간 탐색 후 전환될 수 있다.

따라서 아래 지표를 추가하는 것이 좋다.

- 클릭 후 `complete_subscription`까지 걸린 시간의 median
- 클릭 후 `resubscribe_subscription`까지 걸린 시간의 median
- `start_free_trial -> complete_subscription` 전환 소요 시간
- P25, P50, P75 같은 분포 지표

즉, attribution window를 나누는 것을 넘어, 실제 전환 소요 시간의 분포까지 보는 것이 더 완성도 높은 분석이다.

## 6. 올바른 분석을 위해 추가해야 할 요소

### 6-1. CTA 분석에서 user state 분리

버튼 클릭 시점 기준으로 유저를 최소한 아래처럼 분리해야 한다.

- `never_subscribed`: 클릭 시점 이전에 `complete/renew/resubscribe` 이력이 없음
- `lapsed_user`: 과거 paid event는 있으나 현재는 비활성으로 보는 유저
- `existing_paid_or_historical_user`: paid history가 있는 유저

실무적으로 활성 구독 상태를 정확히 산출하기 어렵다면 최소한 아래 두 그룹은 분리해야 한다.

- 클릭 시점 이전 paid history 없음
- 클릭 시점 이전 paid history 있음

### 6-2. attribution window 정의

버튼 클릭 후 얼마나 이내에 발생한 구독을 전환으로 인정할지 정해야 한다.

추천은 아래처럼 복수 윈도우를 같이 보는 것이다.

- 1일 이내
- 7일 이내
- 30일 이내

### 6-3. denominator를 event 기준과 user 기준으로 분리

같은 유저가 여러 번 페이지에 진입하고 여러 번 버튼을 누를 수 있으므로, 아래 지표를 분리해야 한다.

- 이벤트 기준 전환율
- 유저 기준 전환율

최종 해석은 유저 기준 전환율에 두는 것이 적절하다.

### 6-4. 버튼 비교는 반드시 겹치는 기간만 사용

버튼명이 시기별로 바뀌었다면 전체 기간 비교는 시계열 교란이 된다. 따라서 비교 구간은 아래처럼 맞춰야 한다.

- 버튼 A와 버튼 B가 동시에 존재한 기간만 사용
- 가능하면 같은 월, 같은 콘텐츠, 같은 플랫폼 안에서 비교

### 6-5. 콘텐츠 믹스 통제

버튼명이 달라서 전환이 달라진 것인지, 애초에 버튼이 붙은 콘텐츠가 달라서 전환이 달라진 것인지 분리해야 한다.

최소한 아래 축은 같이 봐야 한다.

- `content.id`
- `content.difficulty`
- 콘텐츠 카테고리 또는 묶음 정보가 있다면 그 분류

### 6-6. 가격/프로모션 통제

버튼 효과와 가격 효과가 뒤섞이지 않게 아래 기준을 함께 봐야 한다.

- `plan.price`
- `paid_amount`
- `coupon.discount_amount`
- 할인율
- 플랜 길이

즉, 버튼명별 전환율만 보면 안 되고, 버튼명별로 어떤 플랜이 팔렸는지도 함께 봐야 한다.

### 6-7. churn 해석 범위 제한

현재 데이터에서 직접 관측 가능한 churn signal은 제한적이다.

- 가능한 것: `click_cancel_plan_button` 기반 voluntary churn proxy
- 어려운 것: payment failure 기반 involuntary churn

따라서 churn 결과 해석 시 "취소 클릭 기반 해지 의도"라는 점을 분명히 적어야 한다.

### 6-8. 통계 검정 단계 추가

세그먼트별/버튼별 차이를 보고 나면 반드시 아래 검증 단계가 들어가야 한다.

- conversion rate 차이에 대한 유의성 검정
- 신뢰구간 계산
- 다중 비교 보정
- 표본 수 하한 설정

### 6-9. 전환 속도와 장기 가치 함께 보기

버튼/프로모션 평가에서 아래 두 축을 함께 봐야 한다.

- 전환 속도: time-to-convert
- 장기 가치: 90일/180일 realized revenue

## 7. 수정된 핵심 가설

### 7-1. 구독 리텐션 관련 가설

- 가설 R1: `complete_subscription` 기준 월별 cohort 간 리텐션은 유의하게 다르다.
- 가설 R2: 특정 cohort는 `renew` 중심 유지보다 `resubscribe` 중심 복귀 비중이 더 높을 수 있다.
- 가설 R3: 12개월 플랜 비중이 높은 cohort는 초기 관측 기간에서 `renew` 패턴이 1개월 플랜 cohort와 다르게 보일 수 있다.
- 가설 R4: 할인 강도가 높은 cohort는 초기 `complete_subscription` 전환은 높지만 이후 realized revenue는 낮을 수 있다.

### 7-2. CTA 신규 전환 관련 가설

- 가설 C1: 같은 관측 기간에서 버튼명에 따라 `content_page -> button click` 비율이 다르다.
- 가설 C2: 버튼명에 따라 `button click -> complete_subscription` 비율이 다르다.
- 가설 C3: 버튼명 효과처럼 보이는 현상의 일부는 실제로 플랜 가격/프로모션 차이일 수 있다.
- 가설 C4: 할인/가성비를 강조하는 버튼은 클릭률은 높지만 90일 realized revenue는 낮을 수 있다.
- 가설 C5: 프리미엄/직접 구독형 버튼은 클릭률은 낮아도 유저 기준 paid conversion quality는 더 높을 수 있다.

### 7-3. CTA 무료 체험 관련 가설

- 가설 T1: 2021-12 ~ 2023-04 기간에 한정하면 버튼명에 따라 `button click -> start_free_trial` 비율이 다를 수 있다.
- 가설 T2: 무료 체험을 많이 유도하는 버튼이 반드시 `start_free_trial -> complete_subscription` 전환율까지 높이지는 않을 수 있다.
- 가설 T3: 다만 free trial 데이터의 관측 기간이 끊겨 있으므로, 이 축은 메인 결론보다 보조 참고 수준으로만 활용하는 것이 적절하다.

### 7-4. CTA 재구독 관련 가설

- 가설 RS1: 버튼명에 따라 `button click -> resubscribe_subscription` 비율이 다르다.
- 가설 RS2: `cancel click` 이력이 있는 유저와 그렇지 않은 유저는 재구독 전환율이 다를 수 있다.

### 7-5. 세그먼트 관련 가설

- 가설 D1: 버튼 효과는 전체 평균보다 `platform`, `device_family`, `device_type`, `city` 같은 세그먼트 안에서 더 크게 다를 수 있다.
- 가설 D2: 특정 세그먼트에서는 무료 체험형 CTA가, 다른 세그먼트에서는 직접 구독형 CTA가 더 효율적일 수 있다.

### 7-6. 전환 속도 관련 가설

- 가설 TT1: 할인형 CTA는 당일 또는 단기 전환 비중이 높을 수 있다.
- 가설 TT2: 프리미엄 가치형 CTA는 전환까지 걸리는 시간은 길지만 최종 결제 품질은 더 높을 수 있다.

## 8. 추천 분석 프레임

### 8-1. 분석 프레임 A: 구독 리텐션

목적은 "첫 구독 이후 paid lifecycle이 얼마나 유지되는가"를 보는 것이다.

- cohort 기준: `complete_subscription` 최초 발생 월
- retention event: 이후 월에 `complete + renew + resubscribe` 중 하나라도 발생
- 단위: 월별 유저 수

### 8-2. 분석 프레임 B: CTA 직접 유료 전환

목적은 "콘텐츠 페이지 버튼이 직접 신규 구독 전환에 미치는 효과"를 보는 것이다.

- `enter_content_page`
- `click_content_page_start_content_button`
- `complete_subscription`

분모는 클릭 시점 이전 paid history가 없는 유저로 두는 것이 적절하다.

### 8-3. 분석 프레임 C: CTA 체험 경유 전환

목적은 "CTA가 무료 체험을 거쳐 실제 결제로 이어지는가"를 제한된 기간에서만 참고용으로 보는 것이다.

- `enter_content_page`
- `click_content_page_start_content_button`
- `start_free_trial`
- `complete_subscription`

이 프레임은 전체 프로젝트의 핵심 분석축이라기보다, free trial 로그가 안정적으로 존재하는 구간에서만 보조 분석으로 다루는 것이 적절하다. 이 경우 아래 두 지표를 따로 본다.

- 체험 시작 전환율
- 체험 이후 paid conversion rate

### 8-4. 분석 프레임 D: CTA 재구독 전환

목적은 "이탈 유저를 다시 결제로 복귀시키는 데 어떤 버튼이 유리한가"를 보는 것이다.

- `enter_content_page`
- `click_content_page_start_content_button`
- `resubscribe_subscription`

분모는 과거 paid history가 있는 유저여야 한다.

### 8-5. 분석 프레임 E: 가격/프로모션 교정 분석

목적은 "버튼명 효과가 실제 CTA 효과인지, 가격/프로모션 효과인지"를 분리하는 것이다.

반드시 함께 봐야 하는 축은 아래와 같다.

- 버튼명
- 관측 기간
- `plan.price`
- `paid_amount`
- 할인율
- 플랜 길이

### 8-6. 분석 프레임 F: churn-rescue 분석

목적은 "취소 의도 이후 어떤 유저가 다시 복귀하는가"를 보는 것이다.

현재 데이터 기준으로는 아래처럼 제한적으로 가능하다.

- `click_cancel_plan_button -> resubscribe_subscription`

단, 이 분석은 voluntary churn proxy 기반임을 명시해야 한다.

### 8-7. 분석 프레임 G: realized LTV 분석

목적은 "초기 전환이 높은 전략이 장기 매출까지 좋은가"를 보는 것이다.

추천 지표는 아래와 같다.

- 90일 누적 `paid_amount`
- 180일 누적 `paid_amount`
- cohort별 realized ARPU
- 버튼명별 realized revenue

### 8-8. 분석 프레임 H: Retention Driver 진단 분석

목적은 "특정 시기에 retention이 높았던 이유가 무엇인가"를 설명하는 것이다.

여기서 중요한 점은, 이번 프로젝트의 retention 정의가 `renew + resubscribe`를 함께 포함한다는 것이다. 따라서 특정 cohort의 retention이 높게 보였을 때, 그것이 아래 중 무엇 때문인지 분리해서 봐야 한다.

- 연속 유지가 강해서 `renew`가 많았는가
- 중간 이탈 후 복귀가 많아서 `resubscribe`가 많았는가
- 애초에 유입된 유저의 질이나 플랜 구조가 달랐는가

권장 진단 축은 아래와 같다.

- `renew retention`과 `resubscribe retention` 분리
- 플랜 길이별 retention 비교
- 할인 강도별 retention 비교
- cohort별 CTA/체험 경로 믹스 비교
- cohort별 초기 콘텐츠 이용량 비교
- cohort별 `cancel click` 발생률 비교
- 세그먼트별 retention 비교

이 프레임을 쓰면 "높은 retention"이 진짜 건강한 유지인지, 아니면 특정 프로모션/재구독 복귀 효과인지 구분할 수 있다.

## 9. 실제 수행 프로세스

### Step 1. 분석 목적 분리

- 리텐션 분석
- CTA 신규 전환 분석
- CTA 체험 경유 전환 분석
- CTA 재구독 전환 분석
- realized LTV 분석

### Step 2. 공통 기간 정렬

- 비교 대상 버튼이 동시에 존재한 기간만 남긴다.
- 퍼널에 들어가는 모든 테이블은 이 기간으로 필터링한다.

### Step 3. 유저 상태 정의

버튼 클릭 시점 이전 paid history를 기준으로 유저를 분리한다.

- 신규 구독 후보
- 재구독 후보
- paid history 보유 유저

### Step 4. 퍼널 연결 규칙 정의

퍼널 연결은 단순 `user_id left join`만으로 보면 안 된다. 아래 규칙이 필요하다.

- 하위 이벤트 시각은 반드시 상위 이벤트 시각보다 늦어야 한다.
- 가능하면 `user_id + content.id + time order`로 본다.
- attribution window를 명시한다.
- 한 유저가 여러 번 클릭한 경우 first-click 기준인지 last-click 기준인지 정한다.

### Step 5. 버튼명별 노출/클릭 비교

먼저 아래를 본다.

- `enter_content_page -> button click`
- 버튼명별 클릭률
- 세그먼트별 클릭률 차이

### Step 6. 버튼명별 무료 체험 시작 비교

그 다음 체험 유도 성과를 본다. 다만 이 단계는 free trial 로그가 안정적으로 존재하는 기간에서만 보조 분석으로 수행한다.

- `button click -> start_free_trial`
- 버튼명별 체험 시작률
- 세그먼트별 체험 시작률

### Step 7. 버튼명별 신규 구독 전환 비교

신규 구독은 별도로 본다.

- 분모: paid history 없는 클릭 유저
- 분자: attribution window 내 `complete_subscription`

### Step 8. 버튼명별 재구독 전환 비교

재구독은 신규 전환과 별도로 본다.

- 분모: 과거 paid history가 있는 클릭 유저
- 분자: attribution window 내 `resubscribe_subscription`

### Step 9. 가격/프로모션 교란 점검

버튼명별로 아래를 같이 본다.

- 어떤 플랜이 선택되었는가
- 평균 `paid_amount`
- 할인액과 할인율
- 플랜 길이

### Step 10. 코호트 리텐션 산출

- `complete_subscription` 최초 월로 cohort 생성
- 이후 월에 union paid event가 있는지로 retention 계산
- cohort별 `renew` 중심 유지인지, `resubscribe` 중심 복귀인지도 분리해서 본다

### Step 10-1. 높은 retention cohort의 원인 진단

특정 시기에 retention이 높게 나왔다면, 바로 "그 시기 전략이 좋았다"라고 결론내리면 안 된다. 아래 순서로 원인을 분해해서 봐야 한다.

1. `renew` 기여와 `resubscribe` 기여를 분리한다.
2. 플랜 길이와 가격 구조가 다른지 본다.
3. 할인 강도와 프로모션 노출이 달랐는지 본다.
4. 무료 체험 경유 비중이 달랐는지 본다.
5. cohort의 초기 engagement 수준이 달랐는지 본다.
6. `cancel click` 비율이 낮았는지 본다.
7. 세그먼트 믹스가 달랐는지 본다.

특히 아래 질문들이 중요하다.

- 높은 retention cohort는 12개월 플랜 비중이 더 높았는가
- 높은 retention cohort는 trial 이후 paid conversion 유저 비중이 달랐는가
- 높은 retention cohort는 첫 7일/30일 내 콘텐츠 소비가 더 깊었는가
- 높은 retention cohort는 `cancel click` 비율이 더 낮았는가
- 높은 retention cohort는 실제 지속 갱신인지, 재구독 복귀 비중 증가인지

즉, retention이 높은 이유를 보려면 단순 retention curve를 넘어서 "cohort 구성과 초기 행동의 차이"를 같이 봐야 한다.

### Step 11. churn-rescue 분석

- `click_cancel_plan_button` 이력이 있는 유저의 재구독률
- cancel click 이후 복귀까지 걸린 시간
- cancel click 유저와 non-cancel 유저의 복귀 차이

단, 이것은 voluntary churn proxy 기반 분석임을 명시한다.

### Step 12. 통계적 유의성 검정

버튼명별/세그먼트별 전환율 차이에 대해 아래를 수행한다.

- 카이제곱 검정 또는 two-proportion z-test
- p-value 산출
- 신뢰구간 계산
- 다중 비교 보정
- minimum sample size 필터

### Step 13. realized LTV 분석

전환율만이 아니라 아래를 함께 본다.

- 90일 realized ARPU
- 180일 realized ARPU
- cohort별 누적 `paid_amount`
- 버튼명별 누적 `paid_amount`

### Step 14. time-to-convert 분석

마지막으로 전환 속도를 본다.

- click -> complete 까지의 median days
- click -> resubscribe 까지의 median days
- free trial -> complete 까지의 median days
- 분포의 분위수 비교

## 10. 최종 권고

현재 구상은 큰 방향에서 맞다. 다만 아래처럼 수정하면 훨씬 논리적으로 단단해진다.

- 리텐션 분석은 `complete cohort + union paid events`로 간다.
- CTA 분석은 `complete`, `resubscribe`, `start_free_trial`을 분리하되, `start_free_trial`은 보조 탐색 축으로만 둔다.
- `renew`는 CTA 직접 전환 endpoint가 아니라 retention/lifecycle 지표로 둔다.
- 버튼 효과는 항상 가격/프로모션 효과와 함께 해석한다.
- 버튼 성과는 전환율뿐 아니라 전환 속도와 realized revenue까지 같이 봐야 한다.
- churn 분석은 현재 데이터에서는 voluntary churn proxy 중심으로 제한적으로 수행한다.
- 높은 retention cohort가 발견되면, 반드시 `renew vs resubscribe`, 플랜 믹스, 할인 강도, 초기 engagement 차이까지 함께 진단해야 한다.

즉, 이번 프로젝트의 핵심 프레임은 아래처럼 정리하는 것이 가장 적절하다.

- 리텐션: `complete -> renew/resubscribe`
- 직접 신규 전환: `content_page -> button click -> complete_subscription`
- 체험 경유 신규 전환: `content_page -> button click -> start_free_trial -> complete_subscription`
  단, 이 경로는 메인 결과가 아니라 제한 기간 보조 분석으로만 사용한다.
- 재획득 전환: `content_page -> button click -> resubscribe_subscription`
- churn-rescue: `cancel click -> resubscribe`
- 장기 가치 검증: `button/profit cohort -> 90/180일 realized revenue`

이렇게 가면 "버튼이 좋아서 전환이 오른 것인지", "가격이 좋아서 오른 것인지", "장기 매출까지 좋은 전략인지"를 메인 분석에서 안정적으로 해석할 수 있고, free trial은 보조적으로만 참고할 수 있다.
