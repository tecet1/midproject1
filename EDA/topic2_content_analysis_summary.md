# 콘텐츠 이용 패턴과 구독 유지 분석 요약

## 1. 분석 목표

본 분석의 목적은 구독 유지 기간이 상이한 유저 집단이 소비하는 콘텐츠의 특성을 비교하고, 이를 통해 장기 구독과 콘텐츠 특성 간의 상관관계를 탐색하는 데 있다.

구체적으로는 다음 질문에 답하고자 하였다.

- 구독 유지 기간이 짧은 유저와 긴 유저는 서로 다른 난이도의 콘텐츠를 소비하는가
- 장기 유지 유저는 평균적으로 더 인기 있는 콘텐츠를 소비하는가
- 연간 플랜으로 시작한 유저와 월간 플랜으로 시작한 유저는 콘텐츠 소비 패턴에서 차이를 보이는가

즉, 본 분석은 단순히 유지 기간의 길이를 측정하는 데 그치지 않고, **유지 그룹별 콘텐츠 소비 특징을 비교하여 장기 구독과 함께 나타나는 콘텐츠 요인을 설명하는 것**을 목표로 한다.

---

## 2. 분석 과정

## 2-1. 유저 마스터 테이블 구축

분석의 기준 테이블로 `complete_subscription_kr_master_v4`를 구축하였다.

구축 과정은 다음과 같다.

1. `complete_subscription`, `renew_subscription`, `resubscribe_subscription` 로그를 정제하였다.
2. 같은 유저의 짧은 기간 반복 결제 시도를 하나의 구매 시도로 압축하였다.
3. 유저별 첫 구독부터 첫 끊김까지의 연속 구독 개월 수(`first_chain_months`)를 계산하였다.
4. 첫 결제 금액 기준으로 시작 플랜 유형(`first_plan_type`)을 월간/연간으로 구분하였다.
5. 회원가입 정보(`signup_time`, `signup_type`, `time_to_first_payment_days`)를 결합하였다.

최종적으로 유저는 다음 4개 그룹(`seg_4grp`)으로 분류하였다.

- `monthly_1m_only`
- `monthly_2m`
- `monthly_3m_plus`
- `annual_start`

이는 리텐션 감소가 실제로 크게 일어나는 초기 구간을 기준으로 월간 유저를 단순 구분하고, 연간 시작 유저는 별도 그룹으로 분리한 것이다.

## 2-2. 콘텐츠 메타 테이블 생성

콘텐츠 분석을 위해 두 가지 메타 정보를 생성하였다.

### 난이도

- `start_content` 로그에서 `content.id`별 가장 마지막 시점의 난이도를 대표값으로 사용
- 컬럼명: `content_difficulty_final`

### 인기도

사분면 분류는 최종적으로 사용하지 않았다.  
대신 콘텐츠별 글로벌 지표 자체를 유지하였다.

- `interest_count`: 전체 `enter_content_page` 이벤트 수
- `start_count`: 전체 `start_content` 이벤트 수

즉, 인기도는 유저 그룹 기준이 아니라 **전체 콘텐츠 로그 기준**으로 정의하였다.

## 2-3. 유저-콘텐츠 결합

이후 `start_content`, `end_content`에 각각 콘텐츠 메타를 결합하였다.

- `start_content_analysis`
- `end_content_analysis`

각 로그는 최종적으로 다음 정보를 포함하게 된다.

- `user_id`
- `content.id`
- `content_difficulty_final`
- `interest_count`
- `start_count`

그리고 이를 유저 마스터의 `seg_4grp`와 `user_id` 기준으로 결합하여 그룹별 집계를 수행하였다.

## 2-4. 집계 방식

난이도는 다음 두 축에서 집계하였다.

- `start_content` 기준 난이도 분포
- `end_content` 기준 난이도 분포

인기도는 두 가지 방식으로 요약하였다.

### 이벤트 기준 평균

그룹이 소비한 전체 콘텐츠 이벤트를 기준으로 `interest_count`, `start_count`의 평균을 계산하였다.

### 유저 기준 평균

유저별로 먼저 자신이 소비한 콘텐츠들의 평균 `interest_count`, 평균 `start_count`를 계산한 뒤, 이를 그룹 단위로 다시 평균하였다.

이 두 방식을 비교함으로써, 일부 헤비 유저의 소비 패턴이 전체 평균을 얼마나 왜곡하는지 점검하였다.

---

## 3. 주요 결과물

본 분석에서 도출한 주요 결과물은 다음과 같다.

### 유저 마스터

- `complete_subscription_kr_master_v4`
- 핵심 컬럼:
  - `user_id`
  - `first_sub_time`
  - `first_chain_months`
  - `first_plan_type`
  - `seg_4grp`
  - `signup_time`
  - `signup_type`
  - `time_to_first_payment_days`

### 콘텐츠 메타

- `content_difficulty_final`
- `content_popularity_metrics`

### 분석용 로그

- `start_content_analysis`
- `end_content_analysis`

### 집계 결과

- 그룹별 난이도 분포
  - `start_difficulty_dist`
  - `end_difficulty_dist`
- 그룹별 인기도 요약
  - `start_popularity_summary`
  - `end_popularity_summary`
- 그룹별 유저 기준 평균 인기도 요약
  - `start_popularity_user_level_summary`
  - `end_popularity_user_level_summary`
- 이벤트 기준 vs 유저 기준 비교
  - `start_popularity_compare`
  - `end_popularity_compare`

---

## 4. 결과 해석

## 4-1. 난이도 분포

`start_content` 기준으로는 모든 유지 그룹에서 `intermediate` 난이도 비중이 가장 높게 나타났다.  
그다음은 `beginner`, `advanced`, `hard` 순이었다.

`end_content` 기준으로는 `beginner` 비중이 상대적으로 확대되고, `advanced` 및 `hard` 비중은 축소되는 경향이 나타났다.

이는 다음과 같이 해석할 수 있다.

- 시작 단계에서는 전반적으로 `intermediate` 난이도 콘텐츠가 핵심 소비 대상이다.
- 완료 단계에서는 상대적으로 난이도가 낮은 콘텐츠가 더 많이 남는다.
- 즉, 어려운 콘텐츠는 시작되더라도 완주까지 이어지는 비중이 낮을 가능성이 있다.

또한 `monthly_3m_plus` 그룹은 `start_content` 기준 `advanced` 비중이 상대적으로 높았으며, `hard` 비중도 소폭 더 높게 나타났다. 반면 `end_content`에서는 `hard` 비중이 매우 낮게 유지되었다.

## 4-2. 인기도 요약

이벤트 기준 평균과 유저 기준 평균을 모두 비교한 결과, 모든 그룹에서 일관되게 유저 기준 평균이 더 높게 나타났다.

예를 들어:

- `start_content` 기준 `interest_count`
- `start_content` 기준 `start_count`
- `end_content` 기준 `interest_count`
- `end_content` 기준 `start_count`

모든 경우에서 `user_level_mean > event_level_mean` 패턴이 관측되었다.

이는 다음과 같이 해석할 수 있다.

- 콘텐츠 소비량이 많은 일부 헤비 유저들이 상대적으로 덜 인기 있는 콘텐츠까지 넓게 소비하고 있다.
- 따라서 이벤트 기준 평균은 헤비 유저의 소비 패턴을 더 강하게 반영한다.
- 반면 유저 기준 평균은 그룹의 평균적 유저 성향을 더 잘 반영한다.

유저 기준 평균 결과를 보면:

- `annual_start` 그룹이 가장 높은 인기도 수준의 콘텐츠를 소비하는 경향이 있었다.
- `monthly_1m_only`가 그 다음이었다.
- `monthly_2m`, `monthly_3m_plus`는 상대적으로 더 niche한 콘텐츠까지 소비하는 경향을 보였다.

즉, 연간 플랜 시작 유저는 상대적으로 인기 콘텐츠 중심 소비 성향을 보이며, 월간 장기 유지 유저는 비교적 덜 인기 있는 콘텐츠까지 소비 범위를 확장하는 것으로 해석할 수 있다.

---

## 5. 한계점

본 분석에는 다음과 같은 한계가 존재한다.

### 5-1. 리텐션 정의의 단순화

현재 유지 그룹은 `first_chain_months`만을 기준으로 정의하였다.  
즉, 첫 구독 이후 첫 끊김 전까지의 연속 구독 길이에 초점을 두었다.

이 방식은 초기 유지 행동을 설명하는 데는 유리하지만, 이후 재구독이나 장기 복귀 패턴은 반영하지 못한다.

### 5-2. 인기도 지표의 해석 한계

`interest_count`와 `start_count`는 콘텐츠 전체 로그 기준으로 정의된 글로벌 지표다.  
따라서 특정 시기별 인기도 변화나 코호트별 노출 차이는 반영하지 못한다.

또한 이벤트 기준 평균은 헤비 유저의 영향을 강하게 받을 수 있으며, 유저 기준 평균은 반대로 라이트 유저를 동일 가중치로 반영한다는 한계가 있다.

### 5-3. 난이도 대표값의 단순화

일부 콘텐츠는 시점에 따라 난이도 레이블이 변경된 이력이 있었다.  
현재는 가장 마지막 시점의 난이도를 대표값으로 사용했으나, 이는 과거 시점의 실제 난이도를 완전히 보존하지는 못한다.

### 5-4. 완료율 분석 제외

`end_content`를 활용한 완강률 분석은 계산 가능했으나, 다음 이유로 메인 분석에서는 제외하였다.

- `end_content`가 완강의 완전한 정의인지 불명확
- 고난도 콘텐츠 구간은 표본 수가 매우 적음
- 해석 안정성이 상대적으로 낮음

따라서 본 분석에서는 완료율보다 난이도 분포와 인기도 요약에 집중하였다.

---

## 6. 최종 정리

본 분석은 다음 흐름으로 진행되었다.

1. 구독 로그 정제
2. 유저 마스터 구축
3. 유지 그룹(`seg_4grp`) 정의
4. 콘텐츠 메타 생성
5. 유저-콘텐츠 결합
6. 난이도 분포 분석
7. 인기도 요약 및 이벤트 기준 vs 유저 기준 비교

현재까지의 결과를 종합하면 다음과 같이 요약할 수 있다.

- 유지 그룹별 콘텐츠 소비 패턴에는 차이가 존재한다.
- 난이도 측면에서는 전반적으로 `intermediate` 중심 시작, `beginner` 중심 완료 경향이 나타난다.
- 인기도 측면에서는 이벤트 기준과 유저 기준 차이가 크게 나타나며, 이는 헤비 유저가 상대적으로 niche한 콘텐츠까지 소비하고 있음을 시사한다.
- 따라서 유지 그룹의 평균적 콘텐츠 소비 성향을 비교하는 목적에는 유저 기준 평균이 더 적합하다.

즉, 본 분석은 장기 구독과 콘텐츠 이용 패턴 간의 관계를 탐색하기 위한 기초 프레임을 구축하였으며, 이후 멘토링에서는 이 구조를 바탕으로 지표 정의의 타당성과 후속 분석 방향을 점검할 수 있다.
