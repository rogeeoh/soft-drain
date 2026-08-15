# 구현 리뷰 1라운드 — 배치·회수 컨트롤러

대상: internal/controller/{softdrain,node_controller,pod_controller}.go, softdrain_test.go, cmd/main.go
리뷰어: 클린코드·테스트 / K8s 설계 정합성 (설계 정합성 리뷰는 도착 대기 중)

## 클린코드·테스트 리뷰 (13건)

### 높음

**C1. 입양된 대체 Pod의 pod-deletion-cost를 아무도 걷지 않는다** — node_controller.go:79
대체 Pod은 생성 시점에 cost min-int를 달고 태어나 다른 노드에 스케줄된다. `restore`는 drain 노드 위 Pod만 훑으므로, 넘기기가 끝난 뒤 사용자 Deployment 안에 "다음 스케일다운에서 항상 제일 먼저 죽는 Pod"이 영구히 남는다. DESIGN.md의 한계는 "원래 값을 복원하지 않는다"이지 "우리 값을 남긴다"가 아님.

→ **처리됨.** cost는 타깃에만 쓰는 것으로 확정 (대체 Pod의 cost는 입양 전엔 무의미, 입양 순간엔 불필요, 입양 후엔 유해). `buildReplacement`에서 cost 제거, DESIGN.md 2단계 문장 명확화, 테스트를 "cost가 없어야 한다"로 반전.

### 중간

**C2. nodesForPod이 API 에러를 로그 없이 삼킨다** — node_controller.go:411, :425
깨우기 유일 통로의 List/Get 실패가 흔적 없이 사라진다. 최소 로그 필요.

**C3. draining / ownedByDeployment / mergePatch 유닛테스트 부재** — softdrain.go:48, :61, :132
특히 `mergePatch`의 nil→JSON null 계약은 라벨·어노테이션 제거의 전 메커니즘인데 검증이 없다.

**C4. "controller가 아닌 ownerRef" 경계 케이스 테스트 부재** — softdrain_test.go:151, :188
`Controller: false`인 ownerRef가 달린 Pod은 유효한 대체 Pod이어야 하는데 그 경계가 테이블에 없다.

**C5. handOver가 hash 없으면 로그 없이 멈춘다** — node_controller.go:278
도달 불가에 가깝지만, 도달하면 어떤 진단 절차로도 안 드러나는 유일한 정지 지점.

**C6. terminating 타깃의 대체 Pod 정리가 두 컨트롤러에 비대칭으로 나뉘어 있다** — node_controller.go:205 vs pod_controller.go:96
동작 결과는 맞지만 "판정은 한 줄로 같다"는 설계 서술과 달리 역할이 갈라져 있고 주석이 없다.

### 낮음

**C7. Scheme 필드 미사용** — 두 Reconciler와 main.go. 스캐폴드 잔재.
**C8. 로그 키 target 값 형식 불일치** — namespace/name과 name이 혼재.
**C9. 이벤트·로그 문구 세부** — 소문자 pod, "Could not" vs "Failed to", 두 문장 짜리 이벤트 note.
**C10. deployHealthy의 nil 역참조가 호출 계약에만 의존** — node_controller.go:372. 가드나 주석 필요.
**C11. NotFound를 캐시에 안 넣어 매 라운드 재조회** — nodeDraining/deployHealthy.
**C12. replsByUID를 한 함수가 변형하고 다음 함수가 읽는다** — 순서가 조용한 전제조건.
**C13. 테스트 테이블 엣지 누락** — deploymentHealthy의 `>=` 경계, podReady의 Unknown.

### 처리 결과 (C2~C13, 전부 반영)

- C2: `nodesForPod` 두 실패 지점에 log.Error. 노드 Get의 NotFound는 정상 경로라 제외.
- C3: `draining` / `ownedByDeployment`(CRD 동명 Kind 포함) / `mergePatch`(nil→JSON null 계약) 테스트 추가.
- C4: `validReplacement`·`replicaSetRef` 테이블에 non-controller ownerRef 경계 추가.
- C5: hash 부재 시 로그 + 도달 불가 근거 주석.
- C6: terminating 타깃 `continue`에 "회수 경로가 같은 판정으로 지운다" 주석.
- C7: 미사용 `Scheme` 필드 제거 (양쪽 Reconciler + main.go 배선).
- C8: 로그 키 target 값을 namespace/name으로 통일.
- C9: "Failed to create replacement Pod"로 통일, 이벤트·로그의 소문자 pod → Pod.
- C10: `deployHealthy`에 nil 가드 + 호출 계약 주석.
- C11: `nodeDraining`/`deployHealthy`가 NotFound 결과도 캐시.
- C12: 트림 의도 주석 (이 라운드의 넘기기가 방금 지운 Pod을 보지 않게).
- C13: `deploymentHealthy` `>=` 경계 3케이스, `podReady` Unknown 케이스 추가.

### 리뷰어가 문제 없음으로 확인한 것

restore 조기 반환 가드, cordon 소유권 처리, 삭제 시 UID Preconditions, main.go 배선.

## K8s 설계 정합성 리뷰 (6건)

**D1. 대체 Pod에도 pod-deletion-cost를 박는다 (높음)** — softdrain.go
C1과 동일 항목. 넘기기 순간 타깃과 동률이 되어 5·6·8순위가 모두 방금 만든 대체 Pod을 가리키므로 타깃 대신 대체 Pod이 지워지고, 매 라운드 Pod 하나씩 태우며 영원히 도는 시나리오까지 짚음.
→ **처리됨.** C1과 함께 해소 — cost는 타깃에만.

**D2. 동시에 두 노드를 drain하면 영구 정지한다 (중간)** — node_controller.go:256
노드 A의 대체 Pod이 노드 B에 앉은 뒤 B에도 drain 라벨이 붙으면: 넘기기는 "drain 노드 착지" 검사에 걸려 영원히 멈추고, PodReconciler는 타깃이 살아 있으니 지우지 않고, Event 문구는 tolerate 워크로드로 오진. 코드는 설계(117행)를 따른 것이므로 설계 쪽 결정 필요.

→ **처리됨.** "drain 노드에 앉은 대체 Pod은 지운다"로 설계 변경 (hash 전이라 누구의 자식도 아니어서 지워도 노출 불변, 재생성은 cordon을 피해 앉음). tolerate 워크로드는 만들고 지우기를 반복하며 반복 Warning Event가 그 사실을 드러냄 — 알려진 한계 갱신. 멈춤 분기와 오진 문구 제거.

**D3. Complete 후 다시 타깃이 생기면 cordon 소유권을 못 되찾는다 (중간, 확신 없음)** — node_controller.go:117, :166
Complete가 어노테이션을 지운 뒤 노드가 다시 InProgress로 돌아오면, cordon 블록은 이미 unschedulable이라 안 돌고 어노테이션이 다시 안 붙는다. 이후 라벨을 걷어도 uncordon되지 않음.

→ **처리됨.** 두 가지 설계 확정으로 시나리오 자체가 사라짐. (1) Complete는 래치 — 붙은 뒤에는 drain 라벨이 걷힐 때까지 관여하지 않으므로 InProgress 역전이 없다. (2) drain 중 uncordon은 취소 — InProgress + !unschedulable 조합을 증거로 감지해 cost 걷고 `state=Cancelled` 래치. cordon이라는 전제가 사라졌는데 재cordon으로 사람과 싸우지 않는다. `drainActive` 헬퍼로 착지 검사·회수 판정에서 Cancelled 노드를 보통 노드로 취급.

**D4. 타깃 판정의 phase 필터가 설계에 없다 (낮음)** — node_controller.go:307
코드는 Failed/Succeeded를 타깃에서 빼는데 DESIGN §2 타깃 정의에는 phase 조건이 없음. 빼지 않으면 Failed Pod이 완료 판정을 영원히 막으므로 코드가 옳고 문서가 빠뜨린 쪽.

→ **처리됨.** DESIGN §2 타깃 정의에 phase 제외와 근거 한 문장 추가. 코드 변경 없음.

**D5. PodReconciler가 Failed 대체 Pod을 지운다 (낮음, 확신 없음)** — pod_controller.go:81
설계는 "세지 않는다"까지만 말하고 지우라고는 안 함. 지우면 반복 사망의 흔적이 `get pods -l replaces`에서 사라져 생성 거부로 오진할 수 있음.

→ **처리됨.** 지우는 게 맞다고 확정 — Pod에는 재시작이 없어(Failed는 터미널, restartPolicy는 컨테이너 수준) 복구는 새 Pod뿐이고, 안 지우면 시체가 무한정 쌓인다. 진단은 kubelet eviction Event와 삭제 로그로 충분. DESIGN §3에 "세지 않고, 지운다"로 명문화. 코드 변경 없음.

**D6. 삭제 직전에 ownerRef를 다시 보지 않는다 (낮음)** — pod_controller.go:50→:71
판정 중에 NodeReconciler가 넘기고 RS가 입양하면, PodReconciler가 방금 입양된 Pod을 UID 일치로 지울 수 있는 좁은 창. 자기치유되지만 "지켜야 할 것 5"가 순간 깨짐.

→ **처리됨.** 모든 대체 Pod 삭제(중복 정리·drain 노드 착지·회수)를 `deleteReplacement`로 통일 — UID+resourceVersion preconditions. 판정 후 hash가 붙었으면 RV가 바뀌어 삭제 거부, 다음 라운드 재판정. 지켜야 할 것 6번으로 명문화.

**문제 없음으로 확인:** 집합 맞추기 판정, Healthy(D) 세 항, 넘기기 전 검사와 순서, 완료 판정의 terminating 포함, hash를 타깃 RS에서 읽기, patch 하나로 라벨 교체, 깨어남 경로 전체, merge patch 내용, APIReader/캐시 구분, RBAC.
