# Topic2 구독 분석 제안서 Revised

## 1. 분석 배경

이 플랫폼은 기본적으로 유료 구독 기반 콘텐츠 플랫폼이다. 따라서 핵심 질문은 일반적인 서비스 퍼널보다 아래 3가지에 집중하는 것이 더 적절하다.

- 어떤 CTA가 신규 구독 전환에 실제로 기여하는가
- 첫 구독 이후 얼마나 유지되고, 어떤 경로로 다시 복귀하는가
- 초기 전환이 장기 매출까지 좋은 전략으로 이어지는가

구독 관련 테이블의 의미는 아래와 같이 해석한다.

- `complete_subscription`: 첫 구독
- `renew_subscription`: 구독 기간 갱신
- `resubscribe_subscription`: 구독 종료 후 재구독
- `start_free_trial`: 무료 체험 시작

이 정의를 기준으로 보면, 이번 프로젝트는 AARRR 전체를 억지로 채우기보다 아래 축에 집중하는 것이 맞다.

- 구독 전환
- 구독 리텐션
- 재구독/복귀
- 보조적으로 무료 체험, 전환 속도, realized revenue

또한 파생변수도 많이 만들기보다 질문에 직접 필요한 최소 수준만 만드는 것이 적절하다.

- cohort month
- 버튼 클릭 시점 기준 user state
- attribution window 내 전환 여부
- 플랜 길이/가격/할인 그룹
- time-to-convert
- voluntary churn proxy 여부

## 2. 초기 분석 방향성

### 2-1. 리텐션 분석

초기 방향성은 다음과 같다.

- `complete_subscription`, `renew_subscription`, `resubscribe_subscription`를 union
- `user_id`별로 timestamp 정렬
- `complete_subscription` 최초 시점을 기준으로 월별 cohort 정의
- 이후 월에 paid event가 다시 발생하는지로 retention 분석

### 2-2. CTA 기반 구독 퍼널 분석

초기 방향성은 다음과 같다.

- `content_page -> button click -> union table of subscription`
- 특정 버튼명이 적용된 기간 중, 겹치는 기간에서 subscription 비율 차이 비교
- `complete_subscription` 또는 button name에 대응하는 구독 전환율 비교
- `plan.price`, `paid_amount` 차이가 존재하므로 가격/프로모션 영향을 함께 고려
- 버튼명에 따라 실제 구독 전환율이 달라졌는지 검토

### 2-3. 추가 검토 포인트

추가로 검토 요청된 방향은 아래와 같다.

- 무료 체험 단계가 있다면 `button click -> free trial -> paid`로 한 단계 더 쪼개기
- churn을 자발적 이탈과 비자발적 이탈로 나눌 수 있는지 검토
- 버튼별 전환율 차이에 대해 통계적 유의성 검정 추가
- 전환율뿐 아니라 3개월/6개월 LTV 또는 누적 ARPU 관점 검토
- attribution window를 넘어서 time-to-convert 분포 분석

## 3. 한계점 및 해결방안

### 3-1. 리텐션 분석의 한계와 해결

초기 리텐션 방향은 전반적으로 타당하다. 다만 retention이 높게 보인 이유를 바로 "좋은 전략"으로 해석하면 위험하다.

한계점:

- `renew`가 많아서 높은 retention인지
- `resubscribe`가 많아서 높은 retention인지
- 플랜 길이, 할인, cohort 구성 차이 때문인지

해결방안:

- retention을 `renew retention`과 `resubscribe retention`으로 분리
- cohort별 플랜 길이, 할인 강도, 무료 체험 경유 비중 비교
- cohort별 초기 engagement, `cancel click` 비율, 세그먼트 믹스까지 함께 진단

### 3-2. CTA 퍼널 정의의 한계와 해결

`content_page -> button click -> union subscription`은 넓게 보면 paid event로 이어지는 흐름이지만, 그대로 쓰면 신규 전환과 유지 이벤트가 섞인다.

한계점:

- `renew_subscription`은 CTA 직접 전환보다 기존 구독 유지의 결과에 가깝다
- 신규 전환과 재구독 전환을 한 endpoint로 묶으면 해석이 흐려진다

해결방안:

- 신규 전환: `button click -> complete_subscription`
- 재획득 전환: `button click -> resubscribe_subscription`
- 리텐션: `complete cohort -> renew/resubscribe`

즉, `renew`는 CTA endpoint가 아니라 retention/lifecycle 지표로 두는 것이 맞다.

### 3-3. 무료 체험 단계의 한계와 해결

무료 체험이 존재한다면 CTA가 직접 유료 전환을 만든 것인지, 무료 체험만 많이 시작시킨 것인지 분리해야 한다.

한계점:

- 무료 체험 시작률이 높은 버튼이 반드시 최종 paid conversion까지 높은 것은 아니다

해결방안:

- 직접 결제 경로: `content_page -> button click -> complete_subscription`
- 체험 경유 경로: `content_page -> button click -> start_free_trial -> complete_subscription`
- 체험 시작률과 체험 후 paid conversion rate를 분리해서 해석

### 3-4. churn 구분의 한계와 해결

자발적/비자발적 이탈 구분은 개념적으로 중요하지만, 현재 데이터의 관측 범위에는 한계가 있다.

한계점:

- `click_cancel_plan_button`은 voluntary churn proxy로 활용 가능
- 결제 실패, 카드 만료, 한도 초과 같은 involuntary churn은 직접 식별하기 어렵다

해결방안:

- 현재 프로젝트에서는 `cancel click -> resubscribe`를 voluntary churn proxy 기반 분석으로 제한
- involuntary churn은 "추가 결제 로그가 있어야 가능한 영역"으로 명시

### 3-5. 버튼 효과 해석의 한계와 해결

버튼명에 따라 전환율이 다르게 보여도, 실제 원인은 버튼이 아니라 가격/플랜/프로모션일 수 있다.

한계점:

- 1개월 vs 12개월 플랜 믹스
- 할인 강도 차이
- 특정 프로모션 기간
- 콘텐츠 믹스 차이

해결방안:

- 겹치는 기간에서만 버튼 비교
- 가능하면 같은 `content.id`, 같은 플랫폼, 같은 세그먼트 안에서 비교
- `plan.price`, `paid_amount`, 할인율, 플랜 길이를 함께 통제

### 3-6. 퍼널 연결 방식의 한계와 해결

단순 `user_id left join`만으로 상위 퍼널과 하위 퍼널을 연결하면 잘못된 전환이 잡힐 수 있다.

한계점:

- 같은 유저가 여러 번 페이지 진입/클릭 가능
- 다른 시점의 구독 이벤트가 잘못 연결될 수 있음

해결방안:

- `하위 이벤트 시각 > 상위 이벤트 시각` 조건 필수
- 가능하면 `user_id + content.id + time order` 기준 사용
- attribution window 명시
- first-click 기준인지 last-click 기준인지 사전 정의

### 3-7. 통계적 검정의 필요성

단순 전환율 차이만으로는 의미 있는 차이인지 판단하기 어렵다.

해결방안:

- 카이제곱 검정 또는 two-proportion z-test 적용
- p-value, 신뢰구간 확인
- 다중 비교 보정
- minimum sample threshold 설정

### 3-8. 장기 가치와 전환 속도 해석의 필요성

초기 전환율이 높아도 장기 매출이 낮을 수 있고, 같은 전환율이라도 전환 속도는 다를 수 있다.

해결방안:

- 90일/180일 realized ARPU 또는 realized LTV proxy 확인
- click 후 `complete`/`resubscribe`까지 걸린 median time-to-convert 분석

## 4. 최종 분석 주제 및 진행 방향 제안

### 4-1. 최종 분석 주제

이번 프로젝트의 최종 분석 주제는 아래처럼 정리하는 것이 가장 적절하다.

1. 첫 구독 cohort 기반 리텐션 분석
2. CTA 기반 신규 구독 전환 분석
3. CTA 기반 무료 체험 경유 전환 분석
4. CTA 기반 재구독 전환 분석
5. 가격/프로모션 교정 분석
6. churn-rescue 분석
7. realized revenue 및 time-to-convert 분석

### 4-2. 권장 진행 순서

#### A. 리텐션 분석

- `complete_subscription` 최초 월로 cohort 생성
- 이후 월의 `complete + renew + resubscribe` 발생 여부로 retention 계산
- `renew`와 `resubscribe` 기여를 분리
- retention이 높은 cohort는 플랜 믹스, 할인, 체험 경유, 초기 engagement 차이까지 진단

#### B. CTA 신규 전환 분석

- 퍼널: `enter_content_page -> button click -> complete_subscription`
- 분모: 클릭 시점 이전 paid history 없는 유저
- 버튼명, 세그먼트, 가격 조건별 전환율 비교

#### C. CTA 무료 체험 경유 분석

- 퍼널: `enter_content_page -> button click -> start_free_trial -> complete_subscription`
- 버튼별 체험 시작률과 체험 후 결제 전환율을 분리해서 해석

#### D. CTA 재구독 전환 분석

- 퍼널: `enter_content_page -> button click -> resubscribe_subscription`
- 분모: 과거 paid history가 있는 유저
- cancel click 이력 유무에 따라 복귀 차이 확인

#### E. 가격/프로모션 교정 분석

- 버튼명별 전환율만 보지 않고 아래를 함께 비교
- `plan.price`
- `paid_amount`
- 할인율
- 플랜 길이

#### F. 검정 및 품질 확인

- 전환율 차이에 대해 유의성 검정 수행
- 90일/180일 realized revenue 확인
- median time-to-convert 확인

### 4-3. 최종 해석 프레임

최종적으로는 아래 질문에 답할 수 있어야 한다.

- 어떤 버튼이 클릭을 많이 만드는가
- 어떤 버튼이 실제 신규 구독을 많이 만드는가
- 어떤 버튼이 무료 체험만 많이 만들고 끝나는가
- 어떤 버튼이 재구독 복귀에 더 효과적인가
- 높은 retention은 진짜 건강한 유지인지, 재구독 복귀 효과인지
- 높은 전환율이 장기 매출까지 좋은 전략인지

즉, 이번 분석은 "버튼이 좋다"를 말하는 것이 아니라, 아래를 분리해서 설명하는 것이 목표다.

- 클릭 효과
- 신규 결제 효과
- 무료 체험 효과
- 재구독 효과
- 리텐션 효과
- 장기 매출 효과
