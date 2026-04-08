# `complete_subscription_kr_master_v4` 테이블 명세서

## 1. 테이블 목적

`complete_subscription_kr_master_v4`는 **유저 단위 구독 마스터 테이블**이다.  
원시 `complete_subscription`, `renew_subscription`, `resubscribe_subscription` 로그를 정제한 뒤, 유저별 첫 구독 정보와 첫 연속 구독 기간을 기준으로 분석용 세그먼트를 생성한 최종 테이블이다.

본 테이블은 이후 다음 분석의 기준 테이블로 사용된다.

- 구독 유지 그룹별 콘텐츠 소비 패턴 비교
- 난이도별 콘텐츠 소비 분포 비교
- 콘텐츠 인기도 지표와 유지 그룹의 관계 분석
- 회원가입 후 첫 결제까지의 기간 비교

---

## 2. 생성 로직 요약

생성 과정은 다음과 같다.

1. `complete_subscription`, `renew_subscription`, `resubscribe_subscription` 로그를 전처리한다.
2. 같은 유저의 짧은 기간 내 반복 결제 시도를 하나의 구매 시도로 압축한다.
3. `complete_clean` 기준으로 유저별 첫 구독 시점과 첫 결제 금액을 추출한다.
4. `complete + renew + resubscribe`를 시간순으로 읽으면서 **첫 번째 연속 구독 체인 길이**를 계산한다.
5. 첫 결제 금액 기준으로 시작 플랜 유형을 월간(`monthly_start`)과 연간(`annual_start`)으로 구분한다.
6. `complete_signup` 로그를 `user_id` 기준으로 결합해 회원가입 관련 컬럼을 추가한다.
7. 첫 연속 구독 길이와 시작 플랜 유형을 기준으로 최종 분석용 세그먼트(`seg_4grp`)를 생성한다.

---

## 3. 테이블 단위

- 단위: **1유저 = 1행**
- 기준: `complete_clean` 기준 첫 구독 유저

즉 동일 유저가 원시 `complete_subscription` 로그를 여러 번 남겼더라도, 본 테이블에서는 첫 구독 기준 1행만 유지된다.

---

## 4. 컬럼 명세

| 컬럼명 | 자료형 | 설명 |
| --- | --- | --- |
| `user_id` | `str` | 유저 식별자 |
| `first_sub_time` | `datetime[μs]` | 중복 결제 시도 정리 후 기준으로 잡은 유저의 첫 구독 시각 |
| `plan.price` | `i64` | 첫 구독 시점의 결제 금액 |
| `first_chain_months` | `i64` 또는 `null` | 첫 구독부터 첫 끊김까지의 연속 구독 개월 수 |
| `consecutive_sub_months` | `i64` 또는 `null` | 기존 분석 코드 호환용 컬럼. 현재는 `first_chain_months`와 동일한 의미 |
| `first_plan_type` | `str` 또는 `null` | 첫 결제 가격 기준 시작 플랜 유형. `monthly_start` 또는 `annual_start` |
| `signup_time` | `datetime[μs]` 또는 `null` | `complete_signup` 기준 회원가입 시각 |
| `signup_type` | `str` 또는 `null` | 회원가입 유형 또는 가입 채널 예: `kakao`, `email` |
| `time_to_first_payment_days` | `f64` 또는 `null` | 회원가입 후 첫 결제까지 걸린 기간(일 단위) |
| `seg_4grp` | `str` 또는 `null` | 최종 분석용 유지 그룹 세그먼트 |

---

## 5. `first_chain_months` 정의

`first_chain_months`는 **첫 구독부터 첫 끊김까지 이어진 첫 번째 연속 구독 체인의 길이(개월 수)**를 의미한다.

예시:

- 첫 구독 1개월 후 추가 결제 없음 -> `1`
- 첫 구독 1개월 + 정상 갱신 1회 -> `2`
- 첫 구독 1개월 + 정상 갱신 2회 -> `3`
- 첫 구독이 연간 플랜 -> `12`

### 연속으로 인정하는 기준

기존 만료일 기준으로,

- `만료 7일 전 ~ 만료 7일 후` 사이 발생한 결제는 같은 체인으로 인정
- 그보다 너무 이른 결제는 중복 시도 또는 겹침 로그로 간주하여 제외
- 그보다 늦은 결제는 첫 번째 연속 구독 체인이 끊긴 것으로 판단

### 포함하는 결제 이벤트

- `complete_subscription`
- `renew_subscription`
- `resubscribe_subscription`

즉 `resubscribe`도 허용 구간 안에 들어오면 첫 체인의 일부로 포함된다.

---

## 6. `first_plan_type` 정의

`first_plan_type`은 첫 구독의 결제 금액(`plan.price`)을 기준으로 정의한다.

- `15920` -> `monthly_start`
- `79200 ~ 131600` -> `annual_start`
- 그 외 -> `null`

이 컬럼은 현재 분석에서 **월간 시작 유저와 연간 시작 유저를 구분하는 핵심 기준**이다.

---

## 7. `seg_4grp` 정의

현재 분석에서는 리텐션 감소가 두드러지게 발생하는 초기 구간을 기준으로 유저를 4개 그룹으로 단순화하였다.

정의는 다음과 같다.

- `monthly_1m_only`
  - `first_plan_type == monthly_start`
  - `first_chain_months == 1`
- `monthly_2m`
  - `first_plan_type == monthly_start`
  - `first_chain_months == 2`
- `monthly_3m_plus`
  - `first_plan_type == monthly_start`
  - `first_chain_months >= 3`
- `annual_start`
  - `first_plan_type == annual_start`

이 정의를 사용한 이유는 다음과 같다.

- 초기 이탈이 실제로 크게 일어나는 1개월차, 2개월차를 직접 구분하기 위함
- 월간 시작 유저의 초기 유지 패턴을 단순하고 해석 가능하게 분류하기 위함
- 연간 시작 유저는 유지 메커니즘이 다르므로 별도 그룹으로 분리하기 위함

---

## 8. `null` 값의 의미

`first_chain_months`, `consecutive_sub_months`, `first_plan_type`, `seg_4grp`가 `null`인 경우는 일반적으로 **첫 결제 가격이 현재 분석 규칙으로 월간/연간에 매핑되지 않는 유저**를 의미한다.

예시로 확인된 비표준 가격:

- `14328`
- `19920`
- `42960`

이러한 케이스는 프로모션 가격, 특수 플랜, 비표준 과금 방식일 가능성이 있으며, 현재 메인 분석에서는 제외하는 것이 적절하다.

중요한 점은 다음과 같다.

- `첫 구독만 하고 갱신하지 않은 유저`는 `null`이 아니라 보통 `first_chain_months = 1` 또는 `12`로 기록된다.
- 따라서 `null`은 “미갱신 유저”가 아니라 “현 규칙으로 해석 불가능한 유저”를 의미한다.

---

## 9. 해석 시 주의사항

### 9-1. `first_chain_months`는 첫 체인 기준 지표

`first_chain_months`는 유저의 평생 총 구독 개월 수를 뜻하지 않는다.  
첫 구독 이후 첫 끊김 전까지의 첫 번째 체인만 반영한다.

예를 들어:

- `1, 2, 3월` 구독
- 이후 gap
- `7, 8, 9, 10월` 재구독

인 경우, 본 테이블에서 `first_chain_months = 3`으로 기록된다.

### 9-2. 연간 시작 유저와 월간 시작 유저는 동일 선상에서 해석하기 어렵다

`annual_start` 그룹은 12개월플랜을 결제한 경우가 반영되므로, `monthly_1m_only`, `monthly_2m`, `monthly_3m_plus`와는 유지 구조 자체가 다르다.

따라서 분석에서는

- 월간 시작 유저 간 비교
- 연간 시작 유저 별도 해석

의 구조를 기본으로 두는 것이 바람직하다.

### 9-3. late cohort 해석 주의

데이터 종료 시점 가까이에 첫 구독한 유저는 이후 유지 여부가 충분히 관측되지 않았을 가능성이 있다.  
다만 현재 세그먼트 정의는 1개월, 2개월, 3개월 이상 중심으로 단순화하였기 때문에, 이전 6개월/12개월 단위 분석보다 late cohort 문제는 상대적으로 완화되었다.

---

## 10. 활용 방향

본 테이블은 다음과 같은 분석에 활용할 수 있다.

### 10-1. 유지 그룹별 콘텐츠 소비 패턴 비교

`seg_4grp`를 기준으로 유저를 구분한 뒤,

- 시작한 콘텐츠의 난이도 분포
- 완료한 콘텐츠의 난이도 분포
- 소비한 콘텐츠의 인기도 수준

을 비교할 수 있다.

### 10-2. 가입 후 결제 전환 속도 비교

`signup_time`, `time_to_first_payment_days`를 활용하여,

- 월간 시작 유저와 연간 시작 유저의 전환 속도 차이
- 유지 그룹별 가입 후 결제까지 걸린 시간 차이

를 비교할 수 있다.

### 10-3. 인기도 지표와 유지 그룹의 관계 분석

콘텐츠별 글로벌 인기도(`interest_count`, `start_count`)를 유저-콘텐츠 로그에 결합한 뒤,

- 이벤트 기준 평균
- 유저 기준 평균

을 각각 계산하여 유지 그룹별 차이를 비교할 수 있다.

---

## 11. 요약

`complete_subscription_kr_master_v4`는 구독 로그를 정제하여 만든 최종 유저 마스터 테이블로, 첫 연속 구독 길이와 시작 플랜 유형을 기준으로 유저를 4개 유지 그룹으로 분류한다.

핵심 포인트는 다음과 같다.

- 분석 단위는 1유저 1행이다.
- `first_chain_months`는 첫 구독부터 첫 끊김까지의 연속 구독 길이다.
- `first_plan_type`은 월간 시작과 연간 시작을 구분한다.
- `seg_4grp`는 현재 분석의 메인 세그먼트 컬럼이다.
- `null`은 미갱신 유저가 아니라 비표준 가격 플랜 유저를 의미한다.

따라서 본 테이블은 **유지 그룹별 콘텐츠 이용 패턴 비교를 위한 기준 마스터 테이블**로 사용할 수 있다.
