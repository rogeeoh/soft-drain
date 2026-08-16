# 리뷰 라운드 — 롤아웃 위임·착지 조기 회수 (2026-08-16)

체크포인트: reconcileReplacements 재구성 (미커밋). 라운드 1/2.

1. [반영됨 — 라운드 접기(사용자 제안): 착지·supersede 삭제가 409면 retry 반환 → drain()이 1초 requeue로 라운드 종료, handOver 미도달. envtest "folds the round" 스펙 추가] [중간] 착지 삭제가 precondition 409로 거부된 Ready 대체가 kept로 재편입 → handOver가
   착지 검사 없이 hash를 붙일 수 있음. 구 코드의 무조건 continue 보호가 이동 중 소실.
   → 제안: handOver에 착지 스킵 가드(삭제 아님, continue만) 복원.
2. [반영됨 — 한계 항목 삭제, 진단 안내에 supersede 원인 추가] [중간] DESIGN.md 자기모순 2곳 — "알려진 한계"의 롤아웃 겹침 항목(이제 사실 아님),
   "막혔을 때 보는 법"의 "대체가 안 보이면 생성 거부"(supersede 억제라는 두 번째 원인 생김).
   → 제안: 두 문단 갱신 (사용자 승인 필요).
3. [반영됨 — supersededByRollout이 paused면 false. e2e paused 스펙 실패가 이 발견을 실증했고, 수정 후 envtest paused 스펙 추가] [낮음] paused + 템플릿 변경: 롤아웃이 실제로 안 도는데 stale 판정 → 무기한 억제, 흔적 없음.
   → 제안: paused면 supersede 규칙 미적용 (전제 "롤아웃이 대체 중"이 깨지므로).
4. [반영됨 — DESIGN 3단계에 Normal Event 명시] [낮음] ReplacementSuperseded 이벤트가 DESIGN.md에 없음. → 제안: 2번 갱신에 한 문장 포함.
5. [반영됨 — NotFound만 통과, 그 외 오류는 실패] [낮음] e2e 조기 회수 단언이 임의의 kubectl 오류를 통과로 침. → 제안: NotFound만 통과로 구분.
6. [반영됨 — Pending 단언을 Eventually 안으로] [낮음/불확실] sd-landing에서 fresh가 land에 잠깐 앉는 이론상 교차 → Pending 단언 flaky 후보.
   → 제안: Pending 단언을 Eventually 안으로 이동.

문제 없음 확인: 무기억·무중단·preconditions·중복 생성 교차·rsCurrent 엣지·이벤트 컨벤션.
